package com.boco.customer.utils;



import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtils {
	private static Properties prop;
	private static InputStream inputStream;
	private static final String path = "/conf/common.properties";
	// 返回配置对象
	public static Properties getConfig(String filePath) {
		
		prop = new Properties();
		try {
			inputStream = ConfigUtils.class.getResourceAsStream(path);
			prop.load(inputStream);
		} catch (Exception e) {
			System.out.println("init properties error: " + path);
			e.printStackTrace();
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					System.out.println("can't close the inputstream!");
					e.printStackTrace();
				}
			}
		}
		return prop;
	}
	
}
