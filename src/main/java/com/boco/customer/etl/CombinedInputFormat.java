package com.boco.customer.etl;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CombinedInputFormat extends CombineFileInputFormat<CombinedInputFormat.WordOffset, Text> {

	public RecordReader<WordOffset, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		return new CombineFileRecordReader<WordOffset, Text>((CombineFileSplit) split, context,
				CombineFileLineRecordReader.class);
	}

	/**
	 * This record keeps &lt;filename,offset&gt; pairs.
	 */
	@SuppressWarnings("rawtypes")
	public static class WordOffset implements WritableComparable {

		private long offset;
		private String fileName;

		public void readFields(DataInput in) throws IOException {
			this.offset = in.readLong();
			this.fileName = Text.readString(in);
		}

		public void write(DataOutput out) throws IOException {
			out.writeLong(offset);
			Text.writeString(out, fileName);
		}

		public int compareTo(Object o) {
			WordOffset that = (WordOffset) o;

			int f = this.fileName.compareTo(that.fileName);
			if (f == 0) {
				return (int) Math.signum((double) (this.offset - that.offset));
			}
			return f;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof WordOffset)
				return this.compareTo(obj) == 0;
			return false;
		}

		@Override
		public int hashCode() {
			assert false : "hashCode not designed";
			return 42; // an arbitrary constant
		}
	}

	/**
	 * RecordReader is responsible from extracting records from a chunk of the
	 * CombineFileSplit.
	 */
	public static class CombineFileLineRecordReader extends RecordReader<WordOffset, Text> {

		private long startOffset; // offset of the chunk;
		private long end; // end of the chunk;
		private long pos; // current pos
		private FileSystem fs;
		private Path path;
		private WordOffset key;
		private Text value;

		private FSDataInputStream fileIn;
		private LineReader reader;

		public CombineFileLineRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index)
				throws IOException {

			fs = FileSystem.get(context.getConfiguration());
			this.path = split.getPath(index);
			this.startOffset = split.getOffset(index);
			this.end = startOffset + split.getLength(index);
			boolean skipFirstLine = false;

			// open the file
			fileIn = fs.open(path);
			if (startOffset != 0) {
				skipFirstLine = true;
				--startOffset;
				fileIn.seek(startOffset);
			}
			reader = new LineReader(fileIn);
			if (skipFirstLine) { // skip first line and re-establish
									// "startOffset".
				startOffset += reader.readLine(new Text(), 0,
						(int) Math.min((long) Integer.MAX_VALUE, end - startOffset));
			}
			this.pos = startOffset;
		}

		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		}

		public void close() throws IOException {
		}

		public float getProgress() throws IOException {
			if (startOffset == end) {
				return 0.0f;
			} else {
				return Math.min(1.0f, (pos - startOffset) / (float) (end - startOffset));
			}
		}

		public boolean nextKeyValue() throws IOException {
			if (key == null) {
				key = new WordOffset();
				key.fileName = path.getName();
			}
			key.offset = pos;
			if (value == null) {
				value = new Text();
			}
			int newSize = 0;
			if (pos < end) {
				newSize = reader.readLine(value);
				pos += newSize;
			}
			if (newSize == 0) {
				key = null;
				value = null;
				return false;
			} else {
				return true;
			}
		}

		public WordOffset getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}
	}
}