package com.boco.customer.hbase.table;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;


public class Spliter {

	public static void createHttpTable(Configuration conf,String table) throws IOException,
			ParseException {
		// 预分Region
		
		RegionHttpSplitter.spliter(conf,table);
		
		
	}
	public static void createHttpTable(String table) throws IOException,
	ParseException {
// 预分Region

		RegionHttpSplitter.spliter(table);

}
	public static void createGnrlTable(String table) throws IOException,
	ParseException {
		// 预分Region

		RegionGnrlSplitter.spliter(table);


}
	public static void createSgsTable(String table) throws IOException,
	ParseException {
		// 预分Region

		RegionSgsSplitter.spliter(table);


}
	public static void createS6aTable(String table) throws IOException,
	ParseException {
		// 预分Region

		RegionS6aSplitter.spliter(table);


}
	public static void createGnHttpTable(Configuration conf,String table) throws IOException,
	ParseException {
// 预分Region

		RegionGnHttpSplitter.spliter(conf,table);


}
	public static void createGnHttpTable(String table) throws IOException,
	ParseException {
// 预分Region

		RegionGnHttpSplitter.spliter(table);


}
	public static void createIndexTable(Configuration conf,String table) throws IOException,
	ParseException {
// 预分Region

		RegionIndexSplitter.spliter(conf,table);


}
	public static void createIndexTable(String table) throws IOException,
	ParseException {
// 预分Region

		RegionIndexSplitter.spliter(table);


}
	public static void createMmeTable(Configuration conf,String table) throws IOException,
	ParseException {
		// 预分Region

		RegionMmeSplitter.spliter(conf,table);


	}
	public static void createMmeTable(String table) throws IOException,
	ParseException {
		// 预分Region

		RegionMmeSplitter.spliter(table);


	}
	public static void createDnsTable(Configuration conf,String table) throws IOException,
	ParseException {
		// 预分Region

		RegionDnsSplitter.spliter(conf,table);


	}
	public static void createDnsTable(String table) throws IOException,
	ParseException {
		// 预分Region

		RegionDnsSplitter.spliter(table);


	}
	public static void createMmsTable(String table) throws IOException,
	ParseException {
		// 预分Region

		RegionMmsSplitter.spliter(table);


	}
	public static void createFtpTable(String table) throws IOException,
	ParseException {
		// 预分Region

		RegionFtpSplitter.spliter(table);


	}
	public static void createEmailTable(String table) throws IOException,
	ParseException {
		// 预分Region

		RegionEmailSplitter.spliter(table);


	}
	public static void createS11Table(String table) throws IOException,
	ParseException {
		// 预分Region

		RegionS11Splitter.spliter(table);


	}
	public static void createRtspTable(String table) throws IOException,
	ParseException {
		// 预分Region

		RegionRtspSplitter.spliter(table);


	}
	public static void createVoipTable(String table) throws IOException,
	ParseException {
		// 预分Region

		RegionVoipSplitter.spliter(table);


	}
	public static void createImTable(String table) throws IOException,
	ParseException {
		// 预分Region

		RegionImSplitter.spliter(table);


	}
	public static void createP2pTable(String table) throws IOException,
	ParseException {
		// 预分Region

		RegionP2pSplitter.spliter(table);


	}
//	public static void createCustrackTable(String table) throws IOException,
//			ParseException {
//		// 预分Region
//		RegionCustrackSplitter.spliter(table);
//	}
	public static void createCustrackTable(String table) throws IOException,
	ParseException {
		// 预分Region

		RegionlteSplitter.spliter(table);


	}
	public static void createCustrackTable(Configuration conf,String table) throws IOException,
	ParseException {
		// 预分Region

		RegionlteSplitter.spliter(conf,table);


	}
}
