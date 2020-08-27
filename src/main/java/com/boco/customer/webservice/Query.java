package com.boco.customer.webservice;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Pattern;

import javax.jws.WebService;
import javax.xml.ws.Endpoint;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import com.boco.customer.utils.ConfigUtils;

@WebService
public class Query {

	private static String quorum;
	private static String clientPort;
	protected static final Logger log = Logger.getLogger(Query.class);
	private static final String config = "/config/hbase.properties";
	// 配置文件
	private static Properties pro = ConfigUtils.getConfig(config);
	public final static byte[] family = "f".getBytes();
	public final static byte[] column = "c".getBytes();
	public static Configuration conf;
	private static Pattern pattern = Pattern.compile("\\|");;// 分隔符
	private static String httpTable = "O_RE_ST_XDR_PS_S1U_HTTP_DETAIL";

	static {
		// 获取参数
		quorum = pro.getProperty("hbase.zookeeper.quorum");
		clientPort = pro.getProperty("hbase.zookeeper.property.clientPort");
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", quorum);
		conf.set("hbase.zookeeper.property.clientPort", clientPort);
	}

	// ------------------------------------------

	/**
	 * HTTP的详单
	 * 
	 * @param tab
	 *            表名
	 * @param msisdns
	 *            手机号码，多个用,分割
	 * @param startTime
	 *            结束时间开始点
	 * @param endTime
	 *            结束时间结束点
	 * @param pageSize
	 *            每頁顯示的記錄數
	 * @param index
	 *            要查询第几页
	 * @return 详单列表
	 */
	public List<String> logQuery(String msisdn, String startTime, String endTime, int pageSize, int index) {
		// 查询详单结果
		List<String> list = getList(msisdn, startTime, endTime);
		if (list.size() == 0) {
			return new ArrayList<String>();
		}
		// 保证分页显示次序
		// Collections.sort(list);
		// 按照时间进行排序，http的详单结束位置在1位
		List<String> order = reverseIndex(list, 1);
		// 分页显示
		List<String> result = pageList(order, pageSize, index);
		return result;
	}

	/**
	 * HTTP的详单查询的记录数
	 * 
	 * @param tab
	 *            表名
	 * @param msisdns
	 *            手机号码，多个用,分割
	 * @param startTime
	 *            结束时间开始点
	 * @param endTime
	 *            结束时间结束点
	 * @param pageSize
	 *            每頁顯示的記錄數
	 * @param index
	 *            要查询第几页
	 * @return 总记录数
	 */
	public int logCount(String msisdns, String startTime, String endTime) {
		// 查询结果
		List<String> list = getList(msisdns, startTime, endTime);
		return list.size();
	}

	/**
	 * 
	 * @param msisdn
	 *            手机号码
	 * @param startTime
	 *            查询开始时间
	 * @param endTime
	 *            查询结束时间
	 * @return 获取查询时间段的上行流量+下行流量+总流量
	 */
	public String dataCount(String msisdn, String startTime, String endTime) {
		// 查询结果
		List<String> list = getList(msisdn, startTime, endTime);
		double ul_data = 0D;
		double dl_data = 0D;
		for (String str : list) {
			String[] lines = str.split("\\|", -1);
			ul_data += Double.parseDouble(lines[41]);
			dl_data += Double.parseDouble(lines[42]);
		}
		return ul_data + "|" + dl_data + "|" + (ul_data + dl_data);
	}

	/**
	 * 
	 * @param msisdn
	 *            手机号码
	 * @param startTime
	 *            查询开始时间
	 * @param endTime
	 *            查询结束时间
	 * @return 根据一级业务+二级业务对流量进行汇总，返回按照流量进行排序的 一级业务-二级业务|业务流量|业务流量占比
	 */

	public List<String> appQuery(String msisdn, String startTime, String endTime) {
		List<String> list = getList(msisdn, startTime, endTime);
		HashMap<String, Double> hs = new HashMap<String, Double>();
		Double totalData = 0d;
		// 根据一级业务+二级业务进行统计
		for (String str : list) {
			String[] lines = str.split("\\|", -1);
			String app_type = lines[22] + "-" + lines[24];// 一级业务-二级业务
			Double data = Double.parseDouble(lines[41]) + Double.parseDouble(lines[42]);
			totalData += data;
			if (hs.containsKey(app_type)) {
				Double tmp = hs.get(app_type);
				tmp += data;
				hs.put(app_type, tmp);
			} else {
				hs.put(app_type, data);
			}
		}
		// 对统计完的数据进行hs转list再按照流量进行排序
		List<String> appList = new ArrayList<String>();
		if (hs.size() > 0) {
			Iterator<Entry<String, Double>> iter = hs.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry<String, Double> entry = (Map.Entry<String, Double>) iter.next();
				String key = (String) entry.getKey();
				Double val = (double) entry.getValue();
				double rat = totalData == 0 ? 0 : val / totalData;
				appList.add(key + "|" + val + "|" + rat);
			}
		} else {
			return appList;
		}
		// 排序
		List<String> result = reverseIndex(appList, 1);
		return result;
	}

	/**
	 * 按照时间对list进行从小到大排序
	 * 
	 * @param list
	 * @param index
	 *            时间在list分割后的位置
	 * @return
	 */
	public static List<String> sortIndex(List<String> list, int index) {
		Map<String, String> map = new IdentityHashMap<String, String>();
		// 把排序的字段生成key
		List<String> list1 = new ArrayList<String>();
		for (int i = 0; i < list.size(); i++) {
			String[] s = pattern.split(list.get(i), -1);
			String k = new String(s[index].replace("[- :]", ""));
			map.put(k, list.get(i));
			list1.add(k);
		}
		// key进行排序
		Collections.sort(list1);
		// 排序后的list
		List<String> order = new ArrayList<String>();
		for (int j = 0; j < list1.size(); j++) {
			order.add(map.get(list1.get(j)));
		}
		return order;
	}

	/**
	 * 按照时间对list进行从大到小排序
	 * 
	 * @param list
	 * @param index
	 *            时间在list分割后的位置
	 * @return
	 */
	public static List<String> reverseIndex(List<String> list, int index) {
		Map<String, String> map = new IdentityHashMap<String, String>();
		// 把排序的字段生成key
		List<String> list1 = new ArrayList<String>();
		int maxlength = 0;
		for (int i = 0; i < list.size(); i++) {
			if (list.get(i).length() > maxlength) {
				maxlength = list.get(i).length();
			}
		}

		for (int i = 0; i < list.size(); i++) {
			String[] s = pattern.split(list.get(i), -1);
			String tmp = s[index];
			while (tmp.length() < maxlength) {
				tmp = "0" + tmp;
			}
			String k = new String(tmp);
			map.put(k, list.get(i));
			list1.add(k);
		}
		// key进行排序
		Collections.sort(list1, Collections.reverseOrder());
		// 排序后的list
		List<String> order = new ArrayList<String>();
		for (int j = 0; j < list1.size(); j++) {
			order.add(map.get(list1.get(j)));
		}
		return order;
	}

	// ---------------------------------------------
	/**
	 * 根据表名和手机号码，开始时间，结束时间获取详单LIST
	 * 
	 * @param tab
	 * @param msisdns
	 * @param startTime
	 * @param endTime
	 * @return
	 * @throws ParseException
	 */
	public static List<String> getList(String msisdn, String startTime, String endTime) {
		List<String[]> tableDates = dateList(startTime, endTime);
		List<String> list = startQuery(httpTable, msisdn, tableDates);
		return list;
	}

	/**
	 * 
	 * @param start
	 * @param end
	 * @return 根据开始时间和结束时间来获取要查询多少天的表
	 */
	public static List<String[]> dateList(String start, String end) {
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
		List<String[]> dates = new ArrayList<String[]>();
		try {
			Date startDate = sdf1.parse(start);
			Date endDate = sdf1.parse(end);
			while (!sdf2.format(startDate).equals(sdf2.format(endDate))) {
				dates.add(new String[] { start, sdf2.format(startDate) + " 23:59:59" });
				start = sdf2.format(addDateOneDay(startDate)) + " 00:00:00";
				startDate = sdf1.parse(start);
			}
			dates.add(new String[] { start, end });
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return dates;
	}

	/**
	 * 
	 * @param date
	 * @return 日期加1
	 */
	public static Date addDateOneDay(Date date) {
		if (null == date) {
			return date;
		}
		Calendar c = Calendar.getInstance();
		c.setTime(date); // 设置当前日期
		c.add(Calendar.DATE, 1); // 日期加1天
		date = c.getTime();
		return date;
	}

	/**
	 * list按照每页显示数返回显示的页
	 * 
	 * @param list
	 * @param pageSize
	 * @param index
	 * @return
	 */
	public static List<String> pageList(List<String> list, int pageSize, int index) {
		List<String> result = new ArrayList<String>();
		int startIndex = (index - 1) * pageSize;
		int endIndex = index * pageSize - 1;
		endIndex = endIndex > list.size() - 1 ? list.size() - 1 : endIndex;
		for (int i = startIndex; i <= endIndex; i++) {
			result.add(list.get(i));
		}
		return result;
	}

	// 查询数据
	public static List<String> startQuery(String table, String msisdn, List<String[]> dates) {
		long bef = System.currentTimeMillis();

		try {
			System.out.println("build cost1:" + (System.currentTimeMillis() - bef));
			long bbb = System.currentTimeMillis();
			HConnection connection = HConnectionManager.createConnection(conf);
			System.out.println("connection cost1:" + (System.currentTimeMillis() - bbb));

			int index = 0;
			long timeout = 5 * 60 * 1000;
			QueryLock lock = new QueryLock(dates.size(), bef);
			Object lockObj = lock.getLock();
			System.out.println("build cost:" + (System.currentTimeMillis() - bef));
			long bef1 = System.currentTimeMillis();
			for (String[] hdate : dates) {
				String start = hdate[0].replaceAll("[- :]", "");
				String end = hdate[1].replaceAll("[- :]", "");
				String tableName = table + start.substring(0, 8);
				Scan scan = new Scan();
				scan.setStartRow(getScanStartOrStop(start, msisdn, (byte) '0'));
				scan.setStopRow(getScanStartOrStop(end, msisdn, (byte) '9'));
				System.out.println("connection:" + connection);
				System.out.println("scan:" + scan);
				new Thread(new DataScanner(lock, connection, scan, (index++) + "", tableName)).start();
			}
			synchronized (lockObj) {
				try {
					lockObj.wait(timeout);
					System.out.println("query timeout:total:" + dates.size() + ",finish:" + lock.getFinish());
				} catch (InterruptedException e) {
					System.out.println("query success:query cost:" + (System.currentTimeMillis() - bef1) + " ms.");
				}
			}
			connection.close();
			return lock.getTotaldata();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new ArrayList<String>();
	}

	/**
	 * 根据号码获取开始key和结束的key
	 * 
	 * @param time
	 * @param msisdn
	 * @param def
	 * @return
	 */
	public static byte[] getScanStartOrStop(String time, String msisdn, byte def) {
		int keyLength = 29;
		byte[] k = new byte[keyLength];
		for (int i = 0; i < k.length; i++) {
			k[i] = def;
		}
		int hashLen = 11;
		int dateLen = 14;
		byte[] hash = stringHashToBytes(msisdn, hashLen);
		byte[] date = formatFullLineDate(time, dateLen);
		System.arraycopy(hash, 0, k, 0, hashLen);
		System.arraycopy(date, 0, k, hashLen, dateLen);
		return k;
	}

	public static byte[] stringHashToBytes(String k, int length) {
		char[] array = k.toCharArray();
		byte[] b = new byte[length];
		int j = 0;
		for (int i = b.length - 1; i >= 0; i--) {
			b[j] = (byte) array[i];
			j++;
		}
		return b;
	}

	public static byte[] formatFullLineDate(String date, int length) {
		String lineDate = date.replaceAll("[ \\-:\\.]", "");
		byte[] rs = lineDate.getBytes();
		if (rs.length == length) {
			return rs;
		} else {
			int copyLen = rs.length < length ? rs.length : length;
			byte[] b = new byte[length];
			for (int i = 0; i < b.length; i++) {
				b[i] = '0';
			}
			System.arraycopy(rs, 0, b, 0, copyLen);
			return b;
		}
	}

	static class QueryLock {
		public long before;
		public int total;
		public int finish;
		public List<String> totaldata = new ArrayList<String>();
		private Object lock;

		public QueryLock(int total, long before) {
			this.before = before;
			this.total = total;
			this.lock = new Object();
		}

		public synchronized void finish(String index, List<String> datas) {
			System.out.println(index + " finish!!");
			totaldata.addAll(datas);
			finish++;
			if (total == finish) {
				finishNotify();
			}
		}

		public void finishNotify() {
			synchronized (lock) {
				System.out.println(total + " all finish,cost " + (System.currentTimeMillis() - before) + " ms.");
				lock.notifyAll();
			}
		}

		public Object getLock() {
			return lock;
		}

		public int getFinish() {
			return finish;
		}

		public List<String> getTotaldata() {
			return totaldata;
		}
	}

	static class DataScanner implements Runnable {

		private Scan scan;
		private String index;
		private QueryLock lock;
		private HConnection connection;
		private String tableName;

		public DataScanner(QueryLock lock, HConnection connection, Scan scan, String index, String tableName) {
			this.scan = scan;
			this.connection = connection;
			this.tableName = tableName;
			this.index = index;
			this.lock = lock;
		}

		public void run() {
			HTable htable = null;
			List<String> datas = new ArrayList<String>();
			try {
				htable = (HTable) connection.getTable(tableName);
				ResultScanner scanner = htable.getScanner(scan);
				Result rs = scanner.next();
				String line = "";
				while (rs != null) {
					line = new String(rs.getValue(family, column));
					datas.add(line);
					rs = scanner.next();
				}
				scanner.close();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (htable != null) {
					try {
						htable.close();
					} catch (IOException e) {
					}
				}
				lock.finish(index, datas);
			}
		}
	}

	public static void main(String[] args) {

		Endpoint.publish("http://" + args[0] + ":" + args[1] + "/QueryServer/QueryPort", new Query());
		log.info("start success!");
	}
}
