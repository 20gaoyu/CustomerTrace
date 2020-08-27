package com.boco.customer.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import com.boco.customer.hbase.HbaseConfigUtils;

public class HdfsToDim {
	public static Properties pro = HbaseConfigUtils.getConfigs();
	static String conf_uri = pro.getProperty("namenode");
	public static HashMap<String, String> getDimInfo(Configuration conf, String dir, String keyIndex, String valueIndex) throws FileNotFoundException, IOException {
		String uri;
		if(!"null".equals(conf_uri)&&conf_uri!=null){
			uri=conf_uri;
		}
		else {
		 uri = "hdfs://10.151.64.10:8020";
		}
	    FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FileStatus[] fstat = fs.listStatus(new Path(dir));
		Path[] listPath = FileUtil.stat2Paths(fstat);
		Long records = 0L;
		HashMap<String, String> dim = new HashMap<String, String>();
		for (Path p : listPath) {
        FSDataInputStream in = fs.open(p);
        BufferedReader bufread = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        String strLine;
        String[] strList;
        
        while ((strLine = bufread.readLine()) != null) {
        	records++;
        	//System.out.println("strLine:"+strLine);
            strList = strLine.split("\\|", -1);
            StringBuffer key = new StringBuffer();
            String[] k = keyIndex.split(",", -1);
            String[] v = valueIndex.split(",", -1);
            for (String i : k) {
            //	System.out.println("k:"+k);
                key.append(strList[Integer.parseInt(i)]).append("|");
            }
            StringBuffer value = new StringBuffer();
            for (String j : v) {
            //	System.out.println("v:"+v);
                value.append(strList[Integer.parseInt(j)]).append("|");
            }
            dim.put(key.toString().substring(0, key.toString().length() - 1), value.toString().substring(0, value.toString().length() - 1));
        }
        in.close();
		}
		System.out.println("名称"+dir+"总记录数"+records);
        return dim;
    }
}
