package com.boco.customer.hbase.table;

import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

import com.boco.customer.hbase.HbaseConfigUtils;

public class RegionMmsSplitter {
	static final Log logger = LogFactory.getLog(RegionMmsSplitter.class);
	// 获取配置文件参数
	private static Properties pro = HbaseConfigUtils.getConfigs();
	// 获取zookeeper的主机
	static String quorum = pro.getProperty("hbase.zookeeper.quorum");
	// 获取zookeeper的主机端口
	static String clientPort = pro.getProperty("hbase.zookeeper.property.clientPort");
	// 分发处理器类
	static String[] coprocessor_class = pro.getProperty("http.coprocessor.class").split(",");
	// 分发处理器地址
	static String[] coprocessor_path = pro.getProperty("http.coprocessor.path").split(",");
	// 获取列族
	static String famliy = pro.getProperty("http.family");
	// 压缩方式
	static String compression = pro.getProperty("http.compression");
	// 表描述方式
	static String tableDescriptor = pro.getProperty("http.descriptor");

	// Region数量
	static int sprangsize = Integer.parseInt(pro.getProperty("http.region.size"));
	// key的长度
	static int keylength = Integer.parseInt(pro.getProperty("http.key.length"));
	private static HBaseAdmin admin;
	private static HTable table;

	public static void spliter(String table) throws IOException, ParseException {
		// 配置HBase参数
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", quorum);
		conf.set("hbase.zookeeper.property.clientPort", clientPort);
		try {
			// 按","切分
			String[] fs = famliy.split(",");
			logger.info("..." + table + "  " + fs + " " + conf);
			// 拆分表
			createPresplitTable(table, fs, conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("deprecation")
	private static void createPresplitTable(String tableName, String[] fs, Configuration conf)
			throws IOException, InterruptedException, ParseException {

		// 预分Region数量
		final int splitCount = sprangsize;
		// 输出建表日志
		logger.debug("Creating table" + tableName + " with " + fs.length + " column families.  Presplitting to "
				+ splitCount + "regions");

		// 定义HTableDescriptor对象，用来分发 coprocessor
		HTableDescriptor desc = new HTableDescriptor(tableName);
		if (coprocessor_path != null && coprocessor_path.length > 10) {
			for (int i = 0; i < coprocessor_path.length; i++) {
				Path path = new Path(coprocessor_path[i]);
				desc.addCoprocessor(coprocessor_class[i], path, Coprocessor.PRIORITY_USER, null);
			}
		}
		// 遍历列族的列 实现压缩
		for (String cf : fs) {
			HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes(cf));
			if (compression != null) {
				if (compression.toUpperCase().equals("SNAPPY")) {
					hcd.setCompressionType(Algorithm.SNAPPY);
				}
				if (compression.toUpperCase().equals("LZO")) {
					hcd.setCompressionType(Algorithm.LZO);
				}
				if (compression.toUpperCase().equals("GZ")) {
					hcd.setCompressionType(Algorithm.GZ);
				}
			}
			if (tableDescriptor != null) {
				if (tableDescriptor.equalsIgnoreCase("PREFIX")) {
					logger.debug("tableType = " + DataBlockEncoding.PREFIX.toString());
					hcd.setDataBlockEncoding(DataBlockEncoding.PREFIX);
				} else if (tableDescriptor.equalsIgnoreCase("DIFF")) {
					logger.debug("tableType = " + DataBlockEncoding.DIFF.toString());
					hcd.setDataBlockEncoding(DataBlockEncoding.DIFF);
				} else if (tableDescriptor.equalsIgnoreCase("FAST_DIFF")) {
					logger.debug("tableType = " + DataBlockEncoding.FAST_DIFF.toString());
					hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
				}
			}
			// 添加压缩后的列
			desc.addFamily(hcd);
		}

		admin = new HBaseAdmin(conf);

		int offset = 999999 / splitCount;
		int base = offset;
		byte[][] splitrange = new byte[splitCount - 1][];
		for (int i = 0; i < splitrange.length; i++) {
			splitrange[i] = formatKey(base);
			base = base + offset;
		}

		// 创建表
		admin.createTable(desc, splitrange);
		// 验证已创建的region总数，如果小于规定的region数量，继续等待region创建，直到创建完成为止
		if (!conf.getBoolean("split.verify", true)) {
			table = new HTable(conf, tableName);
			int onlineRegions = 0;
			while (onlineRegions < splitCount) {
				// 获取已创建的Region数量
				onlineRegions = table.getRegionLocations().size();
				logger.debug(onlineRegions + " of " + splitCount + " regions online... ");
				if (onlineRegions < splitCount) {
					Thread.sleep(10 * 1000); // 等待10秒
				}
			}
		}
		logger.debug("Finished creating table with " + splitCount + "regions");

	}

	private static byte[] formatKey(int base) {
		byte[] b = new byte[6];
		long d = Math.abs(base);
		for (int i = b.length - 1; i >= 0; i--) {
			b[i] = (byte) ((d % 10) + '0');
			d /= 10;
		}
		String all = new String(b) + fzero("", keylength - 6);
		return all.getBytes();

	}

	private static String fzero(String zz, int length) {
		if (zz.length() < length) {
			return fzero(zz + "0", length);
		} else {
			return zz;
		}
	}

}
