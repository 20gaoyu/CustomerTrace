package com.boco.customer.webservice;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import javax.jws.WebService;
import javax.xml.ws.Endpoint;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.generated.master.table_jsp;
import org.apache.hadoop.io.MapWritable;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.openjpa.jdbc.sql.IngresDictionary;

import com.boco.customer.fieldZx.S1MmeZx;
import com.boco.customer.fieldZx.S1uDnsZx;
import com.boco.customer.fieldZx.S1uEmailZx;
import com.boco.customer.fieldZx.S1uFtpZx;
import com.boco.customer.fieldZx.S1uHttpZx;
import com.boco.customer.fieldZx.S1uImZx;
import com.boco.customer.fieldZx.S1uMmsZx;
import com.boco.customer.fieldZx.S1uP2pZx;
import com.boco.customer.fieldZx.S1uRtspZx;
import com.boco.customer.fieldZx.S1uS11Zx;
import com.boco.customer.fieldZx.S1uVoipZx;
import com.boco.customer.utils.ConfigUtils;
import com.boco.customer.utils.DateUtils;
import com.boco.customer.utils.MD5RowKeyGenerator;

@WebService
public class QueryHb {
	//湖北用到方法：xdrQuery，getCount，cellDetail，cellTrack，openHostTtime
	private static String quorum;
	private static String clientPort;
	protected static final Logger log = Logger.getLogger(QueryHb.class);
	private static final String config = "/config/hbase.properties";
	// 配置文件
	private static Properties pro = ConfigUtils.getConfig(config);
	public final static byte[] family = "F".getBytes();
	public final static byte[] column = "COL".getBytes();
	public static Configuration conf;
	private static Pattern pattern = Pattern.compile("\\|");// 分隔符
	private static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static String httpTable = "O_RE_ST_XDR_PS_S1U_KS_HTTP_DETAIL";
	private static Hashtable<Integer, String> map_table=new Hashtable<Integer, String>();
	private static int args_flag=1;
	static {
		// 获取参数
		quorum = pro.getProperty("hbase.zookeeper.quorum");
		clientPort = pro.getProperty("hbase.zookeeper.property.clientPort");
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", quorum);
		conf.set("hbase.zookeeper.property.clientPort", clientPort);
		//对表名做一个映射
		map_table.put(101, "O_RE_ST_XDR_PS_S1U_DNS_DETAIL");
		map_table.put(102, "O_RE_ST_XDR_PS_S1U_MMS_DETAIL");
		map_table.put(103, "O_RE_ST_XDR_PS_S1U_KS_HTTP_DETAIL");
		map_table.put(104, "O_RE_ST_XDR_PS_S1U_FTP_DETAIL");
		map_table.put(105, "O_RE_ST_XDR_PS_S1U_EMAIL_DETAIL");
		map_table.put(106, "O_RE_ST_XDR_PS_S1U_VOIP_DETAIL");
		map_table.put(107, "O_RE_ST_XDR_PS_S1U_RTSP_DETAIL");
		map_table.put(108, "O_RE_ST_XDR_PS_S1U_IM_DETAIL");
		map_table.put(109, "O_RE_ST_XDR_PS_S1U_P2P_DETAIL");
		map_table.put(110, "O_RE_ST_XDR_PS_S1MME_DETAIL");
		map_table.put(111, "O_RE_ST_XDR_PS_S11_DETAIL");
		map_table.put(112, "O_RE_ST_XDR_PS_S1U_HTTP_DETAIL");
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
	public List<String> logDetail(String msisdn, String startTime, String endTime, int pageSize, int index) {
		// 查询详单结果
		List<String> list = getList(103,msisdn, startTime, endTime);
		log.info("list的大小："+list.size());
		List<String> list_23g = getList(112,msisdn, startTime, endTime);
		log.info("list_23g的大小："+list_23g.size());
		
		if (list.size() == 0&&list_23g.size() == 0) {
			list.add(0, "0");
			log.info("list的大小为0："+list.size());
			return list;
		}
		int size = list.size()+list_23g.size();
		// 进行字段过滤
		//List<String> filter_23g = new ArrayList<String>();
		
		List<String> filter = new ArrayList<String>();
		String startTime_1 = "";
		String endTime_1 = "";
		for (String lines : list) {
			StringBuffer sb = new StringBuffer();
			String[] line = lines.split("\\|", -1);
			try {
				startTime_1 = DateUtils.paserTime(line[60], "yyyy-MM-dd HH:mm:ss");
				endTime_1 = DateUtils.paserTime(line[59], "yyyy-MM-dd HH:mm:ss");
			} catch (Exception e) {
				e.printStackTrace();
			}
			sb.append(startTime_1).append("|");// 开始时间
			sb.append(endTime_1).append("|");// 结束时间
			sb.append(line[105]).append("|");// url
			sb.append(line[49]).append("-").append(line[51]).append("|");// app-subapp
			sb.append(line[77]).append("|");// 上行流量
			sb.append(line[78]).append("|");// 下行流量
			sb.append(Double.parseDouble(line[77]) + Double.parseDouble(line[78])).append("|");// 总流量
			sb.append("3").append("|");// 网络类型
			sb.append(line[57]).append("|");// apn
			sb.append(line[10]).append("|");// imei
			sb.append(line[13]).append("|");// 终端品牌
			sb.append(line[14]).append("|");// 终端型号
			sb.append("HTTP");// 业务类型
			filter.add(sb.toString());
		}
		for (String lines_23gs : list_23g) {
			StringBuffer sb_23g = new StringBuffer();
			String[] line_23g = lines_23gs.split("\\|", -1);
			sb_23g.append(line_23g[0]).append("|");// 开始时间
			sb_23g.append(line_23g[1]).append("|");// 结束时间
			sb_23g.append(line_23g[40]).append("|");// url
			sb_23g.append(line_23g[22]).append("-").append(line_23g[24]).append("|");// app-subapp
			sb_23g.append(line_23g[41]).append("|");// 上行流量
			sb_23g.append(line_23g[42]).append("|");// 下行流量
			sb_23g.append(Double.parseDouble(line_23g[41]) + Double.parseDouble(line_23g[42])).append("|");// 总流量
			sb_23g.append(line_23g[8]).append("|");// 网络类型
			sb_23g.append(line_23g[9]).append("|");// apn
			sb_23g.append(line_23g[3]).append("|");// imei
			sb_23g.append(line_23g[6]).append("|");// 终端品牌
			sb_23g.append(line_23g[7]).append("|");// 终端型号
			sb_23g.append("HTTP");// 业务类型
			filter.add(sb_23g.toString());
		}
		// 保证分页显示次序
		// Collections.sort(list);
		// 按照时间进行排序，http的详单结束位置在1位
		log.info("filter的大小为0："+filter.size());
		List<String> order = reverseIndex(filter, 1);
		// 分页显示
		List<String> result = new ArrayList<String>();
		if (pageSize == 0) {
			// 不分页
			result = order;
		} else {
			// 分页显示
			result = pageList(order, pageSize, index);
			result.add(0, size + "");// 分页的增加总记录数
		}
		return result;
	}

	/**
	 * 
	 * @param msisdn
	 * @param startTime
	 * @param endTime
	 * @param pageSize
	 * @param index
	 * @return 返回list，第一行为总记录数
	 */
	public List<String> logDetail_default(String msisdn, String startTime, String endTime, int pageSize, int index) {
		// 查询详单结果

		List<String> list = getList(103,msisdn, startTime, endTime);
		if (list.size() == 0) {
			list.add(0, "0");
			return list;
		}
		int size = list.size();
		// 进行字段过滤
		List<String> filter = new ArrayList<String>();
		for (String lines : list) {
			StringBuffer sb = new StringBuffer();
			String[] line = lines.split("\\|", -1);
			sb.append(line[0]).append("|");// 开始时间
			sb.append(line[1]).append("|");// 结束时间
			sb.append(line[40]).append("|");// url
			sb.append(line[22]).append("-").append(line[24]).append("|");// app-subapp
			sb.append(line[41]).append("|");// 上行流量
			sb.append(line[42]).append("|");// 下行流量
			sb.append(Double.parseDouble(line[41]) + Double.parseDouble(line[42])).append("|");// 总流量
			sb.append(line[8]).append("|");// 网络类型
			sb.append(line[9]).append("|");// apn
			sb.append(line[3]).append("|");// imei
			sb.append(line[6]).append("|");// 终端品牌
			sb.append(line[7]).append("|");// 终端型号
			sb.append("HTTP");// 业务类型
			filter.add(sb.toString());
		}
		// 保证分页显示次序
		// Collections.sort(list);
		// 按照时间进行排序，http的详单结束位置在1位
		List<String> order = reverseIndex(filter, 1);
		List<String> result = new ArrayList<String>();
		if (pageSize == 0) {
			// 不分页
			result = order;
		} else {
			// 分页显示
			result = pageList(order, pageSize, index);
			result.add(0, size + "");// 分页的增加总记录数
		}
		return result;
	}
	public List<String> logQuery(int table_index,String msisdn, String startTime, String endTime, int pageSize, int index) {
		// 查询详单结果
		List<String> list = getList(table_index,msisdn, startTime, endTime);
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
	
	public int xdrDetailQuerycnt(String tab, String msisdns, String startTime,String endTime, int flag) {

		List<String> list = xdrDetailQueryList(tab, msisdns, startTime,
				endTime, flag);
		return list.size();
	}

	/**
	 * 
	 * @param tab
	 *            输入参数为O_RE_ST_XDR_PS_S1U_KS_HTTP_DETAIL、
	 *            O_RE_ST_XDR_PS_S1MME_DETAIL
	 * @param msisdns
	 *            手机号码11位
	 * @param startTime
	 *            开始时间
	 * @param endTime
	 *            结束时间
	 * @param pageSize
	 *            分页大小
	 * @param index
	 *            第几页
	 * @param flag
	 *            0标识全部，1标识异常
	 * @return 返回分页详单
	 */
	public List<String> xdrDetailQuery(String tab, String msisdns,
			String startTime, String endTime, int pageSize, int index, int flag) {

		List<String> list = xdrDetailQueryList(tab, msisdns, startTime,
				endTime, flag);
		// 分页显示
		List<String> result = pageList(list, pageSize, index);
		return result;
	}

	/**
	 * 
	 * @param tab
	 *            输入参数为O_RE_ST_XDR_PS_S1U_KS_HTTP_DETAIL、
	 *            O_RE_ST_XDR_PS_S1MME_DETAIL
	 * @param msisdns
	 *            手机号码11位
	 * @param startTime
	 *            开始时间
	 * @param endTime
	 *            结束时间
	 * @param flag
	 *            0标识全部，1标识异常
	 * @return 返回全部查询的详单
	 */
	public static List<String> xdrDetailQueryList(String tab, String msisdns,
			String startTime, String endTime, int flag) {

		// 查询详单结果
		List<String> list = getList(tab, msisdns, startTime, endTime);
		if (list.size() == 0) {
			return new ArrayList<String>();
		}
		// 通过标识过滤出异常或者全部
		List<String> abnormal = new ArrayList<String>();
		// 保证分页显示次序
		List<String> order = new ArrayList<String>();
		// 按照时间进行排序
		if (tab.equals("O_RE_ST_XDR_PS_S1U_KS_HTTP_DETAIL")) {
			if (flag == 1) {
				abnormal = abnormalFilter(list, 65);//根据http状态过滤app_status
			} else {
				abnormal = list;
			}
			if (abnormal.size() == 0) {
				return new ArrayList<String>();
			}
			order = sortIndex(abnormal, 60);// http的详单结束时间位置在45位
		} else if (tab.equals("O_RE_ST_XDR_PS_S1MME_DETAIL")) {
			if (flag == 1) {
				abnormal = abnormalFilter(list, 19);//proc_status
			} else {
				abnormal = list;
			}
			if (abnormal.size() == 0) {
				return new ArrayList<String>();
			}
			order = sortIndex(abnormal, 18);// S1MME详单结束时间位置在16
		}
		return order;
	}

	/**
	 * 计算号码失败的业务，在小区上的失败次数的统计
	 * 
	 * @param tab
	 *            输入参数为O_RE_ST_XDR_PS_S1U_KS_HTTP_DETAIL(业务)、
	 *            O_RE_ST_XDR_PS_S1MME_DETAIL(信令)
	 * @param msisdns
	 *            手机号码
	 * @param startTime
	 *            开始时间
	 * @param endTime
	 *            结束时间
	 * @return 号码|小区名称|失败次数|成功次數|请求次数
	 */

	public  List<String> failBusiStat(String tab, String msisdns,
			String startTime, String endTime) {

		// 查询详单结果
		List<String> list = getList(tab, msisdns, startTime, endTime);
		if (list.size() == 0) {
			return new ArrayList<String>();
		}
		// 号码、小区、失败次数、成功次数、请求次数
		List<String> failBusiList = new ArrayList<String>();
		// 保证分页显示次序
		List<String> order = new ArrayList<String>();

		// 按照小区统计用户号码的失败次数成功次数以及请求次数
		if (tab.equals("O_RE_ST_XDR_PS_S1U_KS_HTTP_DETAIL")) {
			// 状态码50/app_status、号码11misdn、小区cell_desc33
			failBusiList = cellCount(list, 65, 11, 33);
		} else if (tab.equals("O_RE_ST_XDR_PS_S1MME_DETAIL")) {
			// 状态码19、号码misdn11、小区cell_desc47
			failBusiList = cellCount(list, 19, 11, 47);
		}

		if (failBusiList.size() == 0) {
			return new ArrayList<String>();
		}

		// 按照失败次数排序
		order = reverseIndex(failBusiList, 2);
		return order;
	}

	/**
	 * 计算号码失败的业务，在小区上的失败次数的统计
	 * 
	 * @param tab
	 *            输入参数为O_RE_ST_XDR_PS_S1U_KS_HTTP_DETAIL(业务)、
	 *            O_RE_ST_XDR_PS_S1MME_DETAIL(信令)
	 * @param msisdns
	 *            手机号码
	 * @param startTime
	 *            开始时间
	 * @param endTime
	 *            结束时间
	 * @param codeid
	 *            错误码
	 * @param pageSize
	 *            每页显示记录数
	 * @param index
	 *            第几页
	 * 
	 * @return 错误码详单分页显示
	 */
	public List<String> errCodeList(String tab, String msisdns,
			String startTime, String endTime, String codeid, int pageSize,
			int index) {

		// 查询详单结果
		List<String> list = errCodeList(tab, msisdns, startTime, endTime,
				codeid);
		// 分页显示
		List<String> result = pageList(list, pageSize, index);
		return result;
	}

	/**
	 * 计算号码失败的业务，在小区上的失败次数的统计
	 * 
	 * @param tab
	 *            输入参数为O_RE_ST_XDR_PS_S1U_KS_HTTP_DETAIL(业务)、
	 *            O_RE_ST_XDR_PS_S1MME_DETAIL(信令)
	 * @param msisdns
	 *            手机号码
	 * @param startTime
	 *            开始时间
	 * @param endTime
	 *            结束时间
	 * @param codeid
	 *            错误码
	 * 
	 * @return 错误码详单记录数
	 */
	public int errCodecnt(String tab, String msisdns, String startTime,
			String endTime, String codeid) {

		// 查询详单结果
		List<String> list = errCodeList(tab, msisdns, startTime, endTime,
				codeid);
		return list.size();
	}

	/**
	 * 计算号码失败的业务，在小区上的失败次数的统计
	 * 
	 * @param tab
	 *            输入参数为O_RE_ST_XDR_PS_S1U_KS_HTTP_DETAIL(业务)、
	 *            O_RE_ST_XDR_PS_S1MME_DETAIL(信令)
	 * @param msisdns
	 *            手机号码
	 * @param startTime
	 *            开始时间
	 * @param endTime
	 *            结束时间
	 * @param codeid
	 *            错误码
	 * @param pageSize
	 *            每页显示记录数
	 * @param index
	 *            第几页
	 * 
	 * @return 错误码详单
	 */
	public static List<String> errCodeList(String tab, String msisdns,
			String startTime, String endTime, String codeid) {

		// 查询详单结果
		List<String> list = getList(tab, msisdns, startTime, endTime);
		if (list.size() == 0) {
			return new ArrayList<String>();
		}
		// 异常的业务列表
		List<String> abnormal = new ArrayList<String>();
		// 号码、小区、失败次数
		List<String> errorcode = new ArrayList<String>();
		// 保证分页显示次序
		List<String> order = new ArrayList<String>();
		// 按照时间进行排序
		if (tab.equals("O_RE_ST_XDR_PS_S1U_KS_HTTP_DETAIL")) {
			abnormal = abnormalFilter(list, 65);
			if (abnormal.size() == 0) {
				return new ArrayList<String>();
			} else {
				// http的错误码位置是97
				errorcode = geterrCodeList(abnormal, codeid, 98);
				if (errorcode.size() == 0) {
					return new ArrayList<String>();
				}
				order = sortIndex(errorcode, 60);
			}
		} else if (tab.equals("O_RE_ST_XDR_PS_S1MME_DETAIL")) {
			abnormal = abnormalFilter(list, 19);
			if (abnormal.size() == 0) {
				return new ArrayList<String>();
			} else {
				// mme的错误码位置是18
				errorcode = geterrCodeList(abnormal, codeid, 21);
				if (errorcode.size() == 0) {
					return new ArrayList<String>();
				}
				order = sortIndex(errorcode, 18);
			}
		}
		return order;
	}

	/************************************************************************************************/

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
	public List<String> xdrQuery(String tab, String msisdns, String startTime,
			String endTime, int pageSize, int index) {

		// 查询详单结果
		List<String> list = getList(tab, msisdns, startTime, endTime);
		if (list.size() == 0) {
			return new ArrayList<String>();
		}
		// 保证分页显示次序
		// Collections.sort(list);
		// 按照时间进行排序，http的详单结束位置在6位
		List<String> order = sortIndex(list, 26);
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
	public int getCount(String tab, String msisdns, String startTime,
			String endTime) {
		// 查询结果
		List<String> list = getList(tab, msisdns, startTime, endTime);
		return list.size();
	}

	/**
	 * 獲取时间段在某小区的详单記錄數
	 * 
	 * @param tab
	 * @param msisdns
	 * @param startTime
	 * @param endTime
	 * @param cellName
	 * @param pageSize
	 * @param index
	 * @return
	 */
	public int getCellCount(String tab, String msisdns, String startTime,
			String endTime, String tac,String eci) {
		// 获取小时内的查询结果
		List<String> list = getList(tab, msisdns, startTime, endTime);
		// 通过时间和小区名称对list进行过滤
		List<String> filter = filterList(list, tac,eci, startTime, endTime);
		return filter.size();
	}

	/**
	 * 獲取时间段在某小区的详单，分页返回
	 * 
	 * @param tab
	 * @param msisdns
	 * @param startTime
	 * @param endTime
	 * @param cellName
	 * @return
	 */
	public List<String> cellDetail(String tab, String msisdns,
			String startTime, String endTime, String tac,String eci, int pageSize,
			int index) {
		// 获取小时内的查询结果
		List<String> list_http = getList("LTE_HTTP", msisdns, startTime, endTime);
		List<String> list_mme = getList("LTE_S1MME", msisdns, startTime, endTime);
		List<String> list_dns = getList("LTE_DNS", msisdns, startTime, endTime);
		List<String> list_lte=new ArrayList<String>();
		log.info("cellDetail list_http大小: "+list_http.size()+" list_mme大小: "+list_mme.size()+" list_mme大小: "+list_dns.size());
		for (int i = 0; i < list_http.size(); i++) {
			String[] shttp = pattern.split(list_http.get(i));
			int HTTP_WAP_STATUS=66;
			int succ_sessions_cnt = 0;// 成功次数
			int att_cnt = 0;// 请求次数
			long business_time = 0;// 业务时长
			long dl_data = 0;// 下行流量
			int END_TIME=26;
			int START_TIME=25;
			int DL_DATA=46;
			float business_delay = 0;
			
			// 成功次数
			try {
				if (Integer.valueOf(shttp[HTTP_WAP_STATUS]) >= 200
						&& Integer.valueOf(shttp[HTTP_WAP_STATUS]) < 400) {
					succ_sessions_cnt = 1;
				}
			} catch (Exception e) {
				// TODO: handle exception
				log.info("HTTP_WAP_STATUS异常"+e);
				continue;
			}
			// 连接次数
			if (Integer.valueOf(shttp[HTTP_WAP_STATUS]) > 0) {
				att_cnt = 1;
			}
			// 时长
			business_time = Long.parseLong(shttp[END_TIME])
					- Long.parseLong(shttp[START_TIME]);
			business_delay = att_cnt == 0 ? 0 : business_time / att_cnt;
			// 下行流量
			dl_data = StringUtils.isBlank(shttp[DL_DATA]) ? 0 : Long
					.parseLong(shttp[DL_DATA]);
			String endTime_xdr = DateUtils.paserTime(shttp[END_TIME],
					"yyyyMMddHHmmss");
			String day_id = endTime_xdr.substring(0, 8);
			String hour_id = endTime_xdr.substring(8, 10);
			list_lte.add(day_id+"|" +hour_id+ "|" +shttp[5] + "|" + shttp[7] + "|" + shttp[21] + "|"
					+ shttp[22] + "|" + shttp[6] + "|"+ shttp[START_TIME] + "|"+ shttp[END_TIME]
					+ "|"+ "HTTP流程"+"|"+shttp[HTTP_WAP_STATUS]
					+"|"+succ_sessions_cnt+"|"+att_cnt
					+"|"+business_time+"|"+business_delay+"|"+(StringUtils.isBlank(shttp[35]) ? "0" : shttp[35]));
			//日期|小时|IMSI|MSISDN|TAC|ECI|IMEI|业务流程开始时间|业务流程结束时间|信令/业务过程|原因码|失败原因码描述|成功次数|请求次数|业务时延(ms)|业务时长(ms)|下行流量(Byte)
		}
		for (int j = 0;  j< list_mme.size(); j++) {
			String[] smme = pattern.split(list_mme.get(j));
			int succ_sessions_cnt = 0;// 成功次数
			int att_cnt = 1;// 请求次数
			int PROCEDURE_STATUS=11;//type
			int PROCEDURE_TYPE=8;//type
			int PROCEDURE_STRAT_TIME=9;
			int PROCEDURE_END_TIME=10;
			int FAILURE_CAUSE=13;
			long business_time = 0;// 业务时长
			float business_delay = 0;
			
			// 成功次数
			if (smme[PROCEDURE_STATUS].equals("0")) {
				succ_sessions_cnt = 1;
			}
			// 连接次数
			// att_cnt = 1;
			// 时长
			business_time = Long.parseLong(smme[PROCEDURE_END_TIME])
					- Long.parseLong(smme[PROCEDURE_STRAT_TIME]);
			business_delay = att_cnt == 0 ? 0 : business_time / att_cnt;
			String endTime_xdr = DateUtils.paserTime(smme[PROCEDURE_END_TIME],
					"yyyyMMddHHmmss");
			String day_id = endTime_xdr.substring(0, 8);
			String hour_id = endTime_xdr.substring(8, 10);
			list_lte.add(day_id+"|" +hour_id+ "|" +smme[5] + "|" + smme[7] + "|" + smme[41] + "|"
					+ smme[42] + "|" + smme[6] + "|"+ smme[PROCEDURE_STRAT_TIME] + "|"+ smme[PROCEDURE_END_TIME]
					+ "|"+ smme[PROCEDURE_TYPE]+ "|"+ smme[FAILURE_CAUSE]
					+"|"+succ_sessions_cnt+"|"+att_cnt+"|"+business_delay
					+"|"+business_time+"|"+"0");
			//日期|小时|IMSI|MSISDN|TAC|ECI|IMEI|业务流程开始时间|业务流程结束时间|信令/业务过程|原因码|成功次数|请求次数|业务时延(ms)|业务时长(ms)|下行流量(Byte)
		}
		for (int k = 0;  k< list_dns.size(); k++) {
			String[] sdns = pattern.split(list_dns.get(k));
			int succ_sessions_cnt = 0;// 成功次数
			int att_cnt = 0;// 请求次数
			long business_time = 0;// 业务时长
			int DNS_RESP_CODE= 66;
			int DNS_REQ_CNT=67;
			int END_TIME=26;
			int START_TIME=25;
			float business_delay = 0;
			// 成功次数
			if (sdns[DNS_RESP_CODE].equals("0")) {
				succ_sessions_cnt = 1;
			}
			// 连接次数
			att_cnt = StringUtils.isBlank(sdns[DNS_REQ_CNT]) ? 0 : Integer
					.valueOf(sdns[DNS_REQ_CNT]);
			// 时长
			business_time = Long.parseLong(sdns[END_TIME])
					- Long.parseLong(sdns[START_TIME]);
			business_delay = att_cnt == 0 ? 0 : business_time / att_cnt;
			String endTime_xdr = DateUtils.paserTime(sdns[END_TIME],
					"yyyyMMddHHmmss");
			String day_id = endTime_xdr.substring(0, 8);
			String hour_id = endTime_xdr.substring(8, 10);
			list_lte.add(day_id+"|" +hour_id+ "|" +sdns[5] + "|" + sdns[7] + "|" + sdns[21] + "|"
					+ sdns[22] + "|" + sdns[6] + "|"+ sdns[START_TIME] + "|"+ sdns[END_TIME]
					+ "|"+ "DNS查询"+"|"+sdns[DNS_RESP_CODE]
					+"|"+succ_sessions_cnt+"|"+att_cnt
					+"|"+business_delay+"|"+business_time+"|"+(StringUtils.isBlank(sdns[46]) ? "0" : sdns[46]));
			//日期|小时|IMSI|MSISDN|TAC|ECI|IMEI|业务流程开始时间|业务流程结束时间|信令/业务过程|原因码|成功次数|请求次数|业务时延(ms)|业务时长(ms)|下行流量(Byte)
		}
		// 判断是否查询到结果
		if (list_lte.size() == 0) {
			return new ArrayList<String>();
		}
		// 通过时间和小区名称对list进行过滤
		log.info("cellDetaillte的大小"+list_lte.size());
		/*for(String lin:list_lte)
		{
			log.info("cellTrack行"+lin);
		}*/
		//log.info("cellDetail:tac"+tac+" eci :"+eci);
		List<String> filter = filterList(list_lte, tac,eci, startTime, endTime);
		// 对list进行排序保证分页显示
		// Collections.sort(filter);
		// 按照时间进行排序，轨迹详单位置在18位
		List<String> order = sortIndex(filter, 8);
		// 分页
		List<String> result = pageList(order, pageSize, index);
		//log.info("result.size:"+result.size());
		return result;
	}
	/**
	 * 用户轨迹用户所在小区轨迹查詢
	 * 
	 * @param tab
	 *            表名
	 * @param msisdn
	 *            用戶號碼
	 * @param startTime
	 *            结束时间的开始点
	 * @param endTime
	 *            结束时间的结束点
	 * @return 日期|IMSI|MSISDN|开机附着时间|开机附着TAC|开机附着小区|附着结果

	 */
	public List<String> openHostTtime(String tab, String msisdns, String startTime,
			String endTime) {
		List<String> list_mme = getList(tab, msisdns, startTime, endTime);
		// 判断是否查询到结果
		log.info("openHostTtime的大小"+list_mme.size());
		int PROCEDURE_TYPE=8;
		int KEYWORD1=14;	
		int PROCEDURE_STATUS=11;
		if (list_mme.size() == 0) {
		return new ArrayList<String>();
		}
		// 按照时间进行排序
		List<String> list=new ArrayList<String>();
		for (int j = 0;  j< list_mme.size(); j++) {
			String[] smme = pattern.split(list_mme.get(j));	
			//log.info("host "+smme[PROCEDURE_TYPE]+"|"+smme[KEYWORD1]);
			if(smme[PROCEDURE_TYPE].equals("1")&&smme[KEYWORD1].equals("3")){
				String endTime_xdr = DateUtils.paserTime(smme[9],
						"yyyyMMddHHmmss");
				String day_id = endTime_xdr.substring(0, 8);
				
				list.add(day_id+"|"+smme[5]+"|"+smme[7]+"|"+smme[9]+"|"+smme[41]+"|"+smme[42]+"|"+smme[PROCEDURE_STATUS]);
				//日期|IMSI|MSISDN|开机附着时间|开机附着TAC|开机附着ECI|附着结果
			}
		}
		List<String> orderList = sortIndex(list, 3);
		// 进行合并并截取合适的字段
		List<String> result = new ArrayList<String>();
		return result;
		
	}
	/**
	 * 用户轨迹用户所在小区轨迹查詢
	 * 
	 * @param tab
	 *            表名
	 * @param msisdn
	 *            用戶號碼
	 * @param startTime
	 *            结束时间的开始点
	 * @param endTime
	 *            结束时间的结束点
	 * @return 结束时间开始-结束 小区，经纬度列表
	 */
	public List<String> cellTrack(String tab, String msisdns, String startTime,
			String endTime) {

		List<String> list_http = getList("LTE_HTTP", msisdns, startTime, endTime);
		List<String> list_mme = getList("LTE_S1MME", msisdns, startTime, endTime);
		List<String> list_dns = getList("LTE_DNS", msisdns, startTime, endTime);
		log.info("cellTrack list_http大小: "+list_http.size()+" list_mme大小: "+list_mme.size()+" list_mme大小: "+list_dns.size());
		List<String> list_lte=new ArrayList<String>();
		for (int i = 0; i < list_http.size(); i++) {
			String[] shttp = pattern.split(list_http.get(i));
			list_lte.add(shttp[21] + "|" + shttp[22] + "|" + shttp[25] + "|"
					+ shttp[26] + "|" + "HTTP流程"+"|"+"1");
			//log.info("http : "+shttp[21] + "|" + shttp[22] + "|" + shttp[25] + "|"
			//		+ shttp[26] + "|" + "HTTP流程"+"|"+"1");
			//TAC|ECI|START_TIME|END_TIME|PROTOCOL_TYPE|1
		}
		for (int j = 0;  j< list_mme.size(); j++) {
			String[] smme = pattern.split(list_mme.get(j));
			list_lte.add(smme[41] + "|" + smme[42] + "|" + smme[9] + "|"
					+ smme[10] + "|" + smme[11]+"|"+"2");
			//log.info("mme : "+smme[41] + "|" + smme[42] + "|" + smme[9] + "|"
			//		+ smme[10] + "|" + smme[11]+"|"+"2");
			//TAC|ECI|START_TIME|END_TIME|PROCEDURE_TYPE|2
		}
		for (int k = 0;  k< list_dns.size(); k++) {
			String[] sdns = pattern.split(list_dns.get(k));
			list_lte.add(sdns[21] + "|" + sdns[22] + "|" + sdns[25] + "|"
					+ sdns[26] + "|" + "DNS查询"+"|"+"3");
			//log.info("dns : "+sdns[21] + "|" + sdns[22] + "|" + sdns[25] + "|"
			//		+ sdns[26] + "|" + "DNS查询"+"|"+"3");
			//TAC|ECI|START_TIME|END_TIME|PROCEDURE_TYPE|3
		}
		// 判断是否查询到结果
		if (list_lte.size() == 0) {
			return new ArrayList<String>();
		}
		// 按照时间进行排序
		/*if(args_flag==1){
			for(String lin:list_lte)
			{
				log.info("cellTrack行"+lin);
			}
			}*/
		List<String> orderList = sortIndex(list_lte, 3);
		// 进行合并并截取合适的字段
		List<String> result = new ArrayList<String>();
		
		
		// 获取第一行数据作为比较
		String[] s0 = pattern.split(orderList.get(0));
		String tmpTac = s0[0];
		String tmpEci = s0[1];
		String tmpStart = s0[2];
		String tmpEnd = s0[3];
		//String tmpJd = s0[15];
		//String tmpWd = s0[16];
		String proType = getProType(s0[4]);
		for (int z = 1; z < orderList.size(); z++) {
			String[] s = pattern.split(orderList.get(z));
			String endtime = s[3];
			String starttime = s[2];
			String tac = s[0];
			String eci = s[1];
			String type = getProType(s[3]);
			if (tac.equals(tmpTac)&&eci.equals(tmpEci)) {
				tmpEnd = endtime;
				if (StringUtils.isNotBlank(type) && !proType.contains(type)) {
					proType += proType + type;
				}
			} else {
				result.add(tmpTac + "|" + tmpEci + "|" + tmpStart + "|"
						+ tmpEnd  + "|" + proType);
				tmpTac = tac;
				tmpEci = eci;
				tmpStart = starttime;
				tmpEnd = endtime;
				proType = type;
			}
		}
		// 增加最后一条记录
		result.add(tmpTac + "|" + tmpEci + "|" + tmpStart + "|"
				+ tmpEnd  + "|" + proType);
		return result;
	}

	public static String getProType(String type) {
		String proType = "";
		String regex = "^\\d+$";
		if (type.matches(regex)) {
			if (type.equals("1")) {
				proType = "A";
			} else if (type.equals("6")) {
				proType = "a";
			} else if (type.equals("7")) {
				proType = "P";
			} else if (type.equals("8")) {
				proType = "p";
			} else if (type.equals("5")) {
				proType = "T";
			}
		} else if (type.equals("DNS查询")) {
			proType = "D";
		} else if (StringUtils.isNotBlank(type)) {
			proType = "H";
		}
		return proType;
	}

	/**
	 * 从失败业务列表中统计出号码在各个小区的失败次数
	 * 
	 * @param list
	 *            失败业务列表
	 * @param numIndex
	 *            号码的位置
	 * @param cellIndex
	 *            小区的位置
	 * @return 号码、小区、失败次数
	 */
	public static List<String> getFailBusiList(List<String> list, int numIndex,
			int cellIndex) {
		List<String> lt = new ArrayList<String>();
		Map<String, Integer> hs = new HashMap<String, Integer>();
		for (int i = 0; i < list.size(); i++) {
			String[] line = pattern.split(list.get(i), -1);
			String k = line[numIndex] + "|" + line[cellIndex];
			if (hs.containsKey(k)) {
				hs.put(k, hs.get(k) + 1);
			} else {
				hs.put(k, 1);
			}
		}
		// hashmap转换成list
		Iterator<Entry<String, Integer>> iter = hs.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter
					.next();
			String key = (String) entry.getKey();
			int val = (int) entry.getValue();
			lt.add(key + "|" + val);
		}
		return lt;
	}

	/**
	 * 从失败的业务详单中过滤出指定错误码的详单
	 * 
	 * @param list失败的业务列表
	 * @param code
	 *            错误码
	 * @param codeIndex
	 *            错误码位置
	 * @return 指定错误码的详单
	 */
	public static List<String> geterrCodeList(List<String> list, String code,
			int codeIndex) {
		List<String> lt = new ArrayList<String>();
		for (int i = 0; i < list.size(); i++) {
			String[] line = pattern.split(list.get(i), -1);
			if (line[codeIndex].equals(code)) {
				lt.add(list.get(i));
			}
		}
		return lt;
	}

	/**
	 * 
	 * @param list
	 *            输入list
	 * @param statusIndex
	 *            成功失败的状态字段
	 * @param msisdnIndex
	 *            电话号码所在的位置
	 * @param cellIndex
	 *            小区所在的位置
	 * @return 返回电话号码|小区名称|失败次数|成功次数|请求次数
	 */

	public static List<String> cellCount(List<String> list, int statusIndex,
			int msisdnIndex, int cellIndex) {
		List<String> lt = new ArrayList<String>();
		HashMap<String, int[]> hm = new HashMap<String, int[]>();
		// key：号码|小区 value:失败次数，成功次数，请求次数
		for (int i = 0; i < list.size(); i++) {
			String[] s = pattern.split(list.get(i), -1);
			String key = s[msisdnIndex] + "|" + s[cellIndex];
			// 失败的记录
			if (!s[statusIndex].equals("0")
					&& StringUtils.isNotBlank(s[statusIndex])) {
				if (hm.containsKey(key)) {
					int[] value = { hm.get(key)[0] + 1, hm.get(key)[1],
							hm.get(key)[2] + 1 };
					hm.put(key, value);
				} else {
					int[] value = { 1, 0, 1 };
					hm.put(key, value);
				}

			} else {
				// 成功的记录
				if (hm.containsKey(key)) {
					int[] value = { hm.get(key)[0], hm.get(key)[1] + 1,
							hm.get(key)[2] + 1 };
					hm.put(key, value);
				} else {
					int[] value = { 0, 1, 1 };
					hm.put(key, value);
				}
			}
		}
		// 筛选出失败次数大于零的记录，拼接成号码|小区|失败次数|成功次数|请求次数
		Iterator<Entry<String, int[]>> iter = hm.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, int[]> entry = (Map.Entry<String, int[]>) iter
					.next();
			String key = (String) entry.getKey();
			int[] val = (int[]) entry.getValue();
			// 失败次数大于0
			//if (val[0] > 0) {
				lt.add(key + "|" + val[0] + "|" + val[1] + "|" + val[2]);
			//}
		}
		return lt;
	}

	/**
	 * 
	 * @param list输入list
	 * @param index
	 *            要过滤的字段位置
	 * @return 过滤出过滤位置非0非空的数据
	 */
	public static List<String> abnormalFilter(List<String> list, int index) {
		List<String> lt = new ArrayList<String>();
		for (int i = 0; i < list.size(); i++) {
			String[] s = pattern.split(list.get(i), -1);
			if (!s[index].equals("0") && StringUtils.isNotBlank(s[index])) {
				lt.add(list.get(i));
			}
		}
		return lt;
	}
	/**
	 * 根据表名和手机号码，开始时间，结束时间获取详单LIST
	 * 
	 * @param tab
	 * @param msisdns
	 * @param startTime
	 * @param endTime
	 * @return
	 */

	
	// 将电话号码拆分成数据
	public static List<String> query(String tab, String msis, String start,
			String end) {
		String[] mds = msis.split(",");
		List<String> rs = startQuery_jl(tab, start, end, mds);
		return rs;
	}
	/**
	 * 根据开始时间和结束时间（秒）和小区名称对list进行过滤 时间点在分割后的数据中是18，小区是7
	 * 
	 * @param list
	 *            详单
	 * @param cell
	 *            小区名称
	 * @param startTime
	 *            开始时间
	 * @param endTime
	 *            结束时间
	 * @param index
	 *            小区名称在list详单中的位置
	 * @return
	 */
	public static List<String> filterList(List<String> list, String tac,String eci,
			String startTime, String endTime) {
		List<String> cellall = new ArrayList<String>();
		Date startdate=null;
		Date enddate=null;
		try  
		{  
		    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
		    startdate = sdf.parse(startTime); 
		    enddate = sdf.parse(endTime);
		   
		}  
		catch (ParseException e)  
		{  
			log.info("时间格式不对 "+e.getMessage());  
		} 
		long end = Long.parseLong(endTime.replaceAll("[- :]", ""));
		
		
		// 按照时间过滤小区
		for (int i = 0; i < list.size(); i++) {
			String[] s = pattern.split(list.get(i), -1);
			long time = Long.parseLong(s[7].replaceAll("[- :]", ""));
			//log.info("s[4]:"+s[4]+" tac :"+tac+" eci :"+eci+" time :"+time+" start:"+startdate.getTime()+" end : "+enddate.getTime());
			if (s[4].equals(tac) && s[5].equals(eci) && time >= startdate.getTime() && time <= (enddate.getTime()+999)) {
				cellall.add(list.get(i));
			}
		}
		return cellall;
	}


	// 查询数据
	public static List<String> startQuery_jl(String tableName, String start,
			String end, String[] msisdns) {
		long bef = System.currentTimeMillis();
		HBaseAdmin admin;
		//测试list表
		/*try {
			admin = new HBaseAdmin(conf);
			HTableDescriptor[] htable=admin.listTables();
			for(HTableDescriptor st:htable){
			log.info("HBASE表:"+st.getName());
			
			}
		} catch (MasterNotRunningException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ZooKeeperConnectionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}*/
		 
		

		try {
			log.info("build cost1:"
					+ (System.currentTimeMillis() - bef));
			
			long bbb = System.currentTimeMillis();
			HConnection connection = HConnectionManager.createConnection(conf);
			log.info("connection cost1:"
					+ (System.currentTimeMillis() - bbb));

			int index = 0;
			long timeout = 5 * 60 * 1000;
			QueryLock lock = new QueryLock(msisdns.length, bef);
			Object lockObj = lock.getLock();
			log.info("build cost:"
					+ (System.currentTimeMillis() - bef));
			long bef1 = System.currentTimeMillis();
			for (String msdn : msisdns) {
				Scan scan = new Scan();
				scan.setStartRow(getScanStartTimeHb(start, msdn));
				scan.setStopRow(getScanEndTimeHb(end, msdn));
				// scan.setFilter(new SingleColumnValueFilter(family, column,
				// CompareFilter.CompareOp.EQUAL, msdn.getBytes()));
				log.info("connection:" + connection);
				log.info("scan:" + scan);
				new Thread(new DataScanner(lock, connection, scan, (index++)
						+ "", tableName)).start();
			}
			synchronized (lockObj) {
				try {
					lockObj.wait(timeout);
					log.info("query timeout:total:" + msisdns.length
							+ ",finish:" + lock.getFinish());
				} catch (InterruptedException e) {
					log.info("query success:query cost:"
							+ (System.currentTimeMillis() - bef1) + " ms.");
				}
			}
			connection.close();
			return lock.getTotaldata();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new ArrayList<String>();
	}

	public List<String> logQuerySx(int table_index,String msisdn, String startTime, String endTime, int flag,int pageSize, int index) {
		// 查询详单结果
		List<String> list;
		List<String> list2=new ArrayList<String>();
		String table_name=map_table.get(table_index);
		String table_ind;
		if(flag==2)
		{
		 //使用imsi作为查询条件
		 table_ind=table_name.replace("DETAIL", "INDEX");
		
		 List<String> list1 = getListSx(table_ind,msisdn, startTime, endTime,2);
		 log.info("flag==2的 list1 大小："+list1.size());
		 String start = startTime.replaceAll("[- :]", "");

			
		 String tableName = table_name + start.substring(0, 8);
		 list=getTabList(tableName,list1);
		 log.info("flag==2的 list 大小："+list.size());
		if (list.size() == 0) {
			return new ArrayList<String>();
		}
		
		}
		else
		{
		list = getListSx(table_name,msisdn, startTime, endTime,1);
		log.info("flag==1的 list 大小："+list.size());
		if (list.size() == 0) {
			return new ArrayList<String>();
		}
		}
		//
		List<String> list_zx=null;
		/*switch (table_index) {
		case 101:
			S1uDnsZx svoz_dns=new S1uDnsZx();
			list_zx=svoz_dns.ReturnListZx(list);
			break;
		case 102:
			S1uMmsZx svoz_mms=new S1uMmsZx();
			list_zx=svoz_mms.ReturnListZx(list);
			break;
		case 103:
			S1uHttpZx svoz_http=new S1uHttpZx();
			list_zx=svoz_http.ReturnListZx(list);
			break;
		case 104:
			S1uFtpZx svoz_ftp=new S1uFtpZx();
			list_zx=svoz_ftp.ReturnListZx(list);
			break;
		case 105:
			S1uEmailZx svoz_email=new S1uEmailZx();
			list_zx=svoz_email.ReturnListZx(list);
			break;
		case 106:
			S1uVoipZx svoz_voip=new S1uVoipZx();
			list_zx=svoz_voip.ReturnListZx(list);
			break;
		case 107:
			S1uRtspZx svoz_rtsp=new S1uRtspZx();
			list_zx=svoz_rtsp.ReturnListZx(list);
			break;
		case 108:
			S1uImZx svoz_im=new S1uImZx();
			list_zx=svoz_im.ReturnListZx(list);
			break;
		case 109:
			S1uP2pZx svoz_p2p=new S1uP2pZx();
			
			list_zx=svoz_p2p.ReturnListZx(list);
			break;
		case 110:
			log.info("查询表："+table_name);
			S1MmeZx svoz_mme=new S1MmeZx();
			list_zx=svoz_mme.ReturnListZx(list);
			break;
		case 111:
			S1uS11Zx svoz_s11=new S1uS11Zx();
			list_zx=svoz_s11.ReturnListZx(list);
			break;
		default:
			break;
		}*/
		// 保证分页显示次序
		// Collections.sort(list);
		// 按照时间进行排序，http的详单结束位置在1位
		List<String> order = reverseIndex(list, 1);
		// 分页显示
		List<String> result = pageList(order, pageSize, index);
		return order;
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
	public int logCount(int index,String msisdns, String startTime, String endTime) {
		// 查询结果

		List<String> list = getList(map_table.get(index),msisdns, startTime, endTime);
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
		List<String> list = getList(map_table.get(103),msisdn, startTime, endTime);
		List<String> list_23g = getList(map_table.get(112),msisdn, startTime, endTime);
		
		double ul_data = 0D;
		double dl_data = 0D;
		Long dataTime = 0L;
		DecimalFormat df = new DecimalFormat("###########0.00");
		for (String str : list) {
			String[] lines = str.split("\\|", -1);
			ul_data += Double.parseDouble(lines[77]);
			dl_data += Double.parseDouble(lines[78]);
			dataTime += Long.parseLong(lines[60]) - Long.parseLong(lines[59]);
	
		}
		for (String str : list_23g) {
			String[] lines_23g = str.split("\\|", -1);
			ul_data += Double.parseDouble(lines_23g[41]);
			dl_data += Double.parseDouble(lines_23g[42]);
			try {
				dataTime += sdf1.parse(lines_23g[1]).getTime() - sdf1.parse(lines_23g[0]).getTime();
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return df.format(ul_data / 1024) + "|" + df.format(dl_data / 1024) + "|" + df.format((ul_data + dl_data) / 1024)
				+ "|" + (dataTime / 60000);
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
		List<String> list = getList(map_table.get(103),msisdn, startTime, endTime);
		List<String> list_23g = getList(map_table.get(112),msisdn,startTime, endTime);
		
		HashMap<String, Double> hs = new HashMap<String, Double>();
		Double totalData = 0d;
		// 根据一级业务+二级业务进行统计
		for (String str : list) {
			String[] lines = str.split("\\|", -1);
			String app_type = lines[49] + "-" + lines[51];// 一级业务-二级业务
			Double data = Double.parseDouble(lines[77]) + Double.parseDouble(lines[78]);
			totalData += data;
			if (hs.containsKey(app_type)) {
				Double tmp = hs.get(app_type);
				tmp += data;
				hs.put(app_type, tmp);
			} else {
				hs.put(app_type, data);
			}
		}
		for (String str : list_23g) {
			String[] lines_23g = str.split("\\|", -1);
			String app_type_23g = lines_23g[22] + "-" + lines_23g[24];// 一级业务-二级业务
			Double data_23g = Double.parseDouble(lines_23g[41]) + Double.parseDouble(lines_23g[42]);
			totalData += data_23g;
			if (hs.containsKey(app_type_23g)) {
				Double tmp = hs.get(app_type_23g);
				tmp += data_23g;
				hs.put(app_type_23g, tmp);
			} else {
				hs.put(app_type_23g, data_23g);
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
				double rat = totalData == 0 ? 0 : val / totalData * 100;
				DecimalFormat df = new DecimalFormat("##0.00");
				appList.add(key + "|" + val + "|" + totalData + "|" + df.format(rat) + "%");
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
	public static List<String> getList(int index ,String msisdn, String startTime, String endTime) {
		List<String[]> tableDates = dateList(startTime, endTime);
		String table_name=map_table.get(index);
		List<String> list = startQuery(table_name, msisdn, tableDates,1);
		return list;
	}
	public static List<String> getList(String tab, String msisdns,
			String startTime, String endTime) {
		String day = startTime.replaceAll("[- :]", "").substring(0, 8);
		String table = tab +"_" +day;
		List<String> list = query(table, msisdns, startTime, endTime);
		return list;
	}
	//增加表名的参数//20160615
	public static List<String> getListSx(String table_name,String msisdn, String startTime, String endTime,int flag) {
		List<String[]> tableDates = dateList(startTime, endTime);
		
		List<String> list = startQuery(table_name, msisdn, tableDates,flag);
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
//根据rowkey获取数据
	public synchronized static List<String> getTabList(String tableName, List<String> list) {
		log.info("getTabList的list大小："+list.size());
		List<String> result = new ArrayList<String>();
		HTable htable = null;
		try {
		HConnection connection = HConnectionManager.createConnection(conf); 
		htable = (HTable) connection.getTable(tableName);
		for(String line :list)
		{
			
				String lines = "";
				 Get scan = new Get(line.getBytes());// 根据rowkey查询 
		        Result r = htable.get(scan); 
				if (r != null) {
					lines = new String(r.getValue(family, column));
					result.add(lines);
				   
				}
			}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (htable != null) {
					try {
						htable.close();
					} catch (IOException e) {
					}
				}

			}


		return result;
	}
	// 查询数据
	public static List<String> startQuery(String table, String msisdn, List<String[]> dates,int flag) {
		long bef = System.currentTimeMillis();

		try {
			log.info("build cost1:" + (System.currentTimeMillis() - bef));
			long bbb = System.currentTimeMillis();
			HConnection connection = HConnectionManager.createConnection(conf);
			log.info("connection cost1:" + (System.currentTimeMillis() - bbb));

			int index = 0;
			long timeout = 5 * 60 * 1000;
			QueryLock lock = new QueryLock(dates.size(), bef);
			Object lockObj = lock.getLock();
			log.info("build cost:" + (System.currentTimeMillis() - bef));
			long bef1 = System.currentTimeMillis();
			for (String[] hdate : dates) {
				String start = hdate[0].replaceAll("[- :]", "");
				String end = hdate[1].replaceAll("[- :]", "");
				
				String tableName = table + start.substring(0, 8);
				Scan scan = new Scan();
				if(flag==2)
				{
					
				scan.setStartRow(getScanStartOrStopImsi(start, msisdn, (byte) '0'));
				scan.setStopRow(getScanStartOrStopImsi(end, msisdn, (byte) '9'));
				
				}
				else
				{
					scan.setStartRow(getScanStartOrStop(start, msisdn, (byte) '0'));
					scan.setStopRow(getScanStartOrStop(end, msisdn, (byte) '9'));
				}
				log.info("connection:" + connection);
				log.info("scan:" + scan);
				new Thread(new DataScanner(lock, connection, scan, (index++) + "", tableName)).start();
			}
			synchronized (lockObj) {
				try {
					lockObj.wait(timeout);
					log.info("query timeout:total:" + dates.size() + ",finish:" + lock.getFinish());
				} catch (InterruptedException e) {
					log.info("query success:query cost:" + (System.currentTimeMillis() - bef1) + " ms.");
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
	public static byte[] getScanStartOrStopImsi(String time, String msisdn, byte def) {
		int keyLength = 33;
		byte[] k = new byte[keyLength];
		for (int i = 0; i < k.length; i++) {
			k[i] = def;
		}
		int hashLen = 15;
		int dateLen = 14;
		byte[] hash = stringHashToBytes(msisdn, hashLen);
		byte[] date = formatFullLineDate(time, dateLen);
		System.arraycopy(hash, 0, k, 0, hashLen);
		System.arraycopy(date, 0, k, hashLen, dateLen);
		return k;
	}
	public static byte[] getScanStartTimeHb(String time, String msisdn) {
		int keyLength = 28;
		int misLen = 11;
		int dateLen = 10;
		int dateLen_bu = 3;//时间补位
		//48b18772964997|1473133938857eac47e8099f35c45
		int md5Len = 3;
		int patLen = 1;
		byte[] k = new byte[keyLength];
		byte[] md5hash= new byte[3];
		byte[] pathash= new byte[1];
		byte[] datehash = new byte[10];
		byte[] datehash_bu = new byte[3];
		byte[] msisdnhash = new byte[11];
		Date date=null;
		String dateString = time;
		//String dateString = "2016-08-06 00:00:05";  
		try  
		{  
		    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
		    date = sdf.parse(dateString); 
		    log.info("时间"+date.getTime());
		}  
		catch (ParseException e)  
		{  
		    log.info(e.getMessage());  
		} 
		String md5_str=new MD5RowKeyGenerator().generatePrefix(msisdn).toString();
		String pat_str="|";
		String date_bustr="000";
		datehash_bu=date_bustr.getBytes();
		md5hash=md5_str.getBytes();
		msisdnhash=msisdn.getBytes();
		pathash=pat_str.getBytes();
		datehash=Long.toString(date.getTime()).substring(0,10).getBytes();
		System.arraycopy(md5hash, 0, k, 0, md5Len);
		System.arraycopy(msisdnhash, 0, k, md5Len, misLen);
		System.arraycopy(pathash, 0, k, md5Len+misLen, patLen);
		System.arraycopy(datehash, 0, k, md5Len+misLen+patLen, dateLen);
		System.arraycopy(datehash_bu, 0, k, md5Len+misLen+patLen+dateLen, dateLen_bu);
		return k;
	}
	public static byte[] getScanEndTimeHb(String time, String msisdn) {
		int keyLength = 28;
		int misLen = 11;
		int dateLen = 10;
		int dateLen_bu = 3;//时间补位
		//48b18772964997|1473133938857eac47e8099f35c45
		int md5Len = 3;
		int patLen = 1;
		byte[] k = new byte[keyLength];
		byte[] md5hash= new byte[3];
		byte[] pathash= new byte[1];
		byte[] datehash = new byte[10];
		byte[] datehash_bu = new byte[3];
		byte[] msisdnhash = new byte[11];
		Date date=null;
		String dateString = time;
		//String dateString = "2016-08-06 00:00:05";  
		try  
		{  
		    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
		    date = sdf.parse(dateString); 
		    log.info("时间"+date.getTime());
		}  
		catch (ParseException e)  
		{  
		    log.info(e.getMessage());  
		} 
		String md5_str=new MD5RowKeyGenerator().generatePrefix(msisdn).toString();
		String pat_str="|";
		String date_bustr="999";
		datehash_bu=date_bustr.getBytes();
		md5hash=md5_str.getBytes();
		msisdnhash=msisdn.getBytes();
		pathash=pat_str.getBytes();
		datehash=Long.toString(date.getTime()).substring(0,10).getBytes();
		System.arraycopy(md5hash, 0, k, 0, md5Len);
		System.arraycopy(msisdnhash, 0, k, md5Len, misLen);
		System.arraycopy(pathash, 0, k, md5Len+misLen, patLen);
		System.arraycopy(datehash, 0, k, md5Len+misLen+patLen, dateLen);
		System.arraycopy(datehash_bu, 0, k, md5Len+misLen+patLen+dateLen, dateLen_bu);
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
			log.info(index + " finish!!");
			totaldata.addAll(datas);
			finish++;
			if (total == finish) {
				finishNotify();
			}
		}

		public void finishNotify() {
			synchronized (lock) {
				log.info(total + " all finish,cost " + (System.currentTimeMillis() - before) + " ms.");
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
				
					/*for (KeyValue kv : rs.list())
                    {
                        System.out.println("-------------------------------");
                        System.out.println("rowkey:        " + new String(kv.getRow()));
                        System.out.println("Column Family: " + new String(kv.getFamily()));
                        System.out.println("Column       :" + new String(kv.getQualifier()));
                        System.out.println("value        : " + new String(kv.getValue()));
                        //datas.add(new String(kv.getValue()));
                    }*/
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
		PropertyConfigurator.configure("conf/log4j.properties");
		Endpoint.publish("http://" + args[0] + ":" + args[1] + "/QueryServer/QueryPort", new QueryHb());
		/*String msisdn="13689270000";
		String startTime="2016-07-24 00:20:49";
		String endTime="2016-07-24 23:25:49";
		//测试
		System.out.println("start 程序!");
		QueryHb qSx=new QueryHb();
		String msisdn="13971676810";
		String startTime="2016-09-03 12:00:00";
		String endTime="2016-09-03 16:59:59";
		String table_name="LTE_HTTP";
		String tac="28984";
		String eci="111569922";
		int flag_index=2;
		int pageSize=10;
		int index=1;
		int flag=1;
		log.info("查询函数!");*/
		if(args.length==3){
			args_flag=0;
		}
		log.info("args_flag"+args_flag);
		//int cou=qSx.getCount(table_name,msisdn,startTime,endTime);
		
		/*List<String> list1=qSx.openHostTtime(table_name,msisdn,startTime,endTime);
		for(String line:list1)
		{
		System.out.println("openHostTtime"+line);
		}*/
		/*List<String> list2=qSx.cellDetail(table_name,msisdn,startTime,endTime,tac,eci,pageSize,index);
		//List<String> list1=failBusiStat(httpTable,msisdn,startTime,endTime);
		for(String line:list2)
			{
			System.out.println("cellDetail"+line);
			}
		xdrQuery(String tab, String msisdns, String startTime,
				String endTime, int pageSize, int index)
		if(args.length==7)
		{
		table_flag=Integer.parseInt(args[0]);
		msisdn=args[1];
		startTime=args[2];
		endTime=args[3];
		pageSize=Integer.parseInt(args[4]);
		index=Integer.parseInt(args[5]);
		flag=Integer.parseInt(args[6]);
		}	*/	
		//log.info("count记录数 :"+cou);
		
		log.info("start success!");
		
	}
}
