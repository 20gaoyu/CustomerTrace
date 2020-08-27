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

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.commons.lang.StringUtils;

import javax.jws.WebService;
import javax.xml.ws.Endpoint;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.generated.master.table_jsp;
import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.booleanValue_return;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.openjpa.jdbc.sql.IngresDictionary;

import com.boco.customer.fieldJl.S1uS6a;
import com.boco.customer.fieldZx.S1MmeCauseZx;
import com.boco.customer.fieldZx.S1SgsZx;
import com.boco.customer.fieldZx.S1uGnrlZx;
import com.boco.customer.fieldZx.S1MmeZx;
import com.boco.customer.fieldZx.S1uDnsZx;
import com.boco.customer.fieldZx.S1uEmailZx;
import com.boco.customer.fieldZx.S1uFtpZx;
import com.boco.customer.fieldZx.S1uHttpCauseZx;
import com.boco.customer.fieldZx.S1uHttpZx;
import com.boco.customer.fieldZx.S1uImZx;
import com.boco.customer.fieldZx.S1uMmsZx;
import com.boco.customer.fieldZx.S1uP2pZx;
import com.boco.customer.fieldZx.S1uRtspZx;
import com.boco.customer.fieldZx.S1uS11Zx;
import com.boco.customer.fieldZx.S1uVoipZx;
import com.boco.customer.fieldZx.S6aZx;
import com.boco.customer.utils.ConfigUtils;
import com.boco.customer.utils.DateUtils;

@WebService
public class QuerySx {

	private static String quorum;
	private static String clientPort;
	protected static final Logger log = Logger.getLogger(QuerySx.class);
	private static final String config = "/config/hbase.properties";
	// 配置文件
	private static Properties pro = ConfigUtils.getConfig(config);
	public final static byte[] family = "f".getBytes();
	public final static byte[] column = "c".getBytes();
	public static Configuration conf;
	private static String kerberos_count ;
	private static String if_kerberos ;
	private static Pattern pattern = Pattern.compile("\\|");// 分隔符
	private static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static String httpTable = "O_RE_ST_XDR_PS_S1U_KS_HTTP_DETAIL";
	private static Hashtable<Integer, String> map_table=new Hashtable<Integer, String>();
	private static Hashtable<String, Integer> feild_table=new Hashtable<String, Integer>();
	static {
		// 获取参数
		quorum = pro.getProperty("hbase.zookeeper.quorum");
		clientPort = pro.getProperty("hbase.zookeeper.property.clientPort");
		kerberos_count = pro.getProperty("kerberos_count");
		if_kerberos = pro.getProperty("if_kerberos");
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
		map_table.put(113, "O_RE_ST_XDR_PS_S1U_GNRL_DETAIL");
		map_table.put(114, "O_RE_ST_XDR_PS_S6A_DETAIL");
		map_table.put(115, "O_RE_ST_XDR_PS_SGS_DETAIL");
		
		//吉林端到端添加
		map_table.put(116, "O_RE_ST_XDR_PS_S1U_HTTPCAUSE_DETAIL");
		map_table.put(117, "O_RE_ST_XDR_PS_S1MMECAUSE_DETAIL");
		feild_table.put("APP_DESC", 49);
		feild_table.put("SUBAPP_DESC", 51);
		if("true".equals(if_kerberos))
		{
		log.info("进行kerberos认证，账号："+kerberos_count);
		System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
		conf.set("keytab.file", "/home/boco/boco.keytab");
		conf.set("hadoop.security.authentication", "kerberos");
		conf.set("hbase.security.authentication", "kerberos");
		 //设置hbase master及hbase regionserver的安全标识，这两个值可以在hbase-site.xml中找到
		 conf.set("hbase.master.kerberos.principal", "hbase/_HOST@HADOOP.COM");
		 conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@HADOOP.COM");
		 UserGroupInformation.setConfiguration(conf);
		}
	}

	/**
	 * HTTP的用户粒度指标分组求和
	 * 
	 * @param tab
	 *            表名
	 * @param msisdns
	 *            手机号码，多个用,分割
	 * @param startTime
	 *            结束时间开始点
	 * @param endTime
	 *            结束时间结束点
	 * @return 用户粒度指标汇总数据
	 */
	public List<String> httpUserGroup(String msisdn, String startTime, String endTime) {
		// 查询详单结果
		List<String> list = getList(103,msisdn, startTime, endTime);
		log.info("list的大小："+list.size());
		log.info("数据："+list.get(0));
		if (list.size() == 0) {
			list.add(0, "0");
			log.info("list的大小为0："+list.size());
			return list;
		}
		int size = list.size();
		String DAY_ID = null;
		String HOUR_ID = null;
		String IMSI = null;
		String MSISDN= null;
		String  tcp_conn_status=null;
		long HTTP_WAP_STATUS;
		double TCP_ATT_CNT=0;
		double TCP_SUCC_CNT=0;
		double 	TCP_SYNACK23_CNT=0;
		double 	TCP_SYNACK12_CNT=0;
		double 	TCP_RESPONSE_DELAY=0;
		double 	TCP_CONFIRM_DELAY=0;
		double 	FIRST_HTTP_RES_DELAY=0;
		double 	HTTP_RSP_FAIL_CNT=0;
		double 	BUSS_DUR=0;
		double 	HTTP_ATT_CNT=0;
		double 	DL_DATA=0;
		double 	UL_DATA=0;
		double 	SUCC_PAGEDISPLAY_CNT=0;
		double CONTENT_LENGTH=0;
		double BUSS_SUCC_CNT=0;//新加
		double BUSS_ATT_CNT=0;//新加
		double BUSS_DELAY=0;//新加
		double HTTP_SUCC_CNT=0;//新加
		double HTTP_FAIL_CNT=0;//新加
		Map<String,List<Double>> tMap = new HashMap<String,List<Double>>();
		List<String> filter_return = new ArrayList<String>();
		int i=0,j=0,k=0;
		for (String lines : list) {
			
			List<Double> filter = new ArrayList<Double>();
			String[] line = lines.split("\\|", -1);
			String keyGroupBy="-1";
			//log.info(lines);
			//log.info("HTTP_WAP_STATUS:"+line[98]+"tcp_conn_status:"+line[94]+"TCP_RESPONSE_DELAY: "+line[87]+"TCP_CONFIRM_DELAY:"+line[88]
			//		+"FIRST_HTTP_RES_DELAY:"+line[101]);
			try {
				DAY_ID = line[0];
				HOUR_ID =line[1];
				IMSI=line[9];
				MSISDN=line[11];
				if(DAY_ID==null||HOUR_ID==null||IMSI==null||MSISDN==null)
				{
					continue;
				}
				keyGroupBy="DAY_ID:"+DAY_ID+"|"+"HOUR_ID:"+HOUR_ID+"|"+"IMSI:"+IMSI+"|"+"MSISDN:"+MSISDN;
				try {
				    HTTP_WAP_STATUS = Long.parseLong(addDefault(line[98], "0"));
				    CONTENT_LENGTH=Double.parseDouble(addDefault(line[111].trim(), "0"));
				} catch (NumberFormatException e1) {
				    HTTP_WAP_STATUS=-1;
				    CONTENT_LENGTH=0;
				}
				tcp_conn_status= addDefault(line[94].trim(), "0");
				BUSS_ATT_CNT=1;
				if(line[93]!=null)
				 	TCP_ATT_CNT=Double.parseDouble(line[93]);
				if (tcp_conn_status.equals("0")) {
					TCP_SUCC_CNT = TCP_ATT_CNT;
					BUSS_SUCC_CNT=1;
				}
				if(HTTP_WAP_STATUS>=300){
					HTTP_RSP_FAIL_CNT=1;
				   }
			    
				if(line[87]!=null)
				{
					i++;
					TCP_RESPONSE_DELAY=Double.parseDouble(addDefault(line[87].trim(), "0"));
				}
				if(line[88]!=null)
				{
					j++;
					TCP_CONFIRM_DELAY=Double.parseDouble(addDefault(line[88].trim(), "0"));
				}
				if(line[101]!=null)
				{
					k++;
					FIRST_HTTP_RES_DELAY=Double.parseDouble(addDefault(line[101].trim(), "0"));
				}
				UL_DATA=Double.parseDouble(addDefault(line[77].trim(), "0"));
				DL_DATA=Double.parseDouble(addDefault(line[78].trim(), "0"));
				HTTP_ATT_CNT=1;
				if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
					if((UL_DATA+DL_DATA)>CONTENT_LENGTH){
						SUCC_PAGEDISPLAY_CNT=1;  
					}
				    }
				if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
					HTTP_SUCC_CNT=1;
				    }
				else
				{
					HTTP_FAIL_CNT=1;
				}
				if(TCP_CONFIRM_DELAY>0){
				    	TCP_SYNACK12_CNT =1; 
					   }
					if(TCP_RESPONSE_DELAY>0){
					    TCP_SYNACK23_CNT =1; 
					}
				 	
				BUSS_DUR=Long.parseLong(line[60].trim())- Long.parseLong(line[59].trim()) ;
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			filter.add(TCP_ATT_CNT);
			filter.add(TCP_SUCC_CNT);
			filter.add(TCP_SYNACK23_CNT);
			filter.add(TCP_SYNACK12_CNT);
			filter.add(TCP_RESPONSE_DELAY);
			filter.add(TCP_CONFIRM_DELAY);
			filter.add(FIRST_HTTP_RES_DELAY);
			filter.add(HTTP_RSP_FAIL_CNT);
			filter.add(BUSS_DUR);
			filter.add(HTTP_ATT_CNT);
			filter.add(DL_DATA);
			filter.add(SUCC_PAGEDISPLAY_CNT);
			filter.add(UL_DATA);
			filter.add(BUSS_SUCC_CNT);
			filter.add(BUSS_ATT_CNT);
			filter.add(HTTP_SUCC_CNT);
			filter.add(HTTP_FAIL_CNT);
			if(tMap.containsKey(keyGroupBy)){
				tMap.get(keyGroupBy).set(0,(tMap.get(keyGroupBy).get(0)+TCP_ATT_CNT));
				tMap.get(keyGroupBy).set(1,(tMap.get(keyGroupBy).get(1)+TCP_SUCC_CNT));
				tMap.get(keyGroupBy).set(2,(tMap.get(keyGroupBy).get(2)+TCP_SYNACK23_CNT));
				tMap.get(keyGroupBy).set(3,(tMap.get(keyGroupBy).get(3)+TCP_SYNACK12_CNT));
				tMap.get(keyGroupBy).set(4,(tMap.get(keyGroupBy).get(4)+TCP_RESPONSE_DELAY));
				tMap.get(keyGroupBy).set(5,(tMap.get(keyGroupBy).get(5)+TCP_CONFIRM_DELAY));
				tMap.get(keyGroupBy).set(6,(tMap.get(keyGroupBy).get(6)+FIRST_HTTP_RES_DELAY));
				tMap.get(keyGroupBy).set(7,(tMap.get(keyGroupBy).get(7)+HTTP_RSP_FAIL_CNT));
				tMap.get(keyGroupBy).set(8,(tMap.get(keyGroupBy).get(8)+BUSS_DUR));
				tMap.get(keyGroupBy).set(9,(tMap.get(keyGroupBy).get(9)+HTTP_ATT_CNT));
				tMap.get(keyGroupBy).set(10,(tMap.get(keyGroupBy).get(10)+DL_DATA));
				tMap.get(keyGroupBy).set(11,(tMap.get(keyGroupBy).get(11)+SUCC_PAGEDISPLAY_CNT));
				tMap.get(keyGroupBy).set(12,(tMap.get(keyGroupBy).get(12)+UL_DATA));
				tMap.get(keyGroupBy).set(13,(tMap.get(keyGroupBy).get(13)+BUSS_SUCC_CNT));
				tMap.get(keyGroupBy).set(14,(tMap.get(keyGroupBy).get(14)+BUSS_ATT_CNT));
				tMap.get(keyGroupBy).set(15,(tMap.get(keyGroupBy).get(15)+HTTP_SUCC_CNT));
				tMap.get(keyGroupBy).set(16,(tMap.get(keyGroupBy).get(16)+HTTP_FAIL_CNT));
				
			
			}else{
				tMap.put(keyGroupBy,filter);
			}

		}
		Iterator iter = tMap.entrySet().iterator();
		while (iter.hasNext()) {
		Map.Entry entry = (Map.Entry) iter.next();
		Object key = entry.getKey();
		List<Double> val = (List<Double>)entry.getValue();
		StringBuffer sb = new StringBuffer();
		sb.append(key.toString()).append("|");
		if(i!=0)
		{
			val.set(4,val.get(4)/i);
		}
		else
		{
			val.set(4,0.0);
		}
		if(j!=0)
		{
			val.set(5,val.get(5)/j);
		}
		else
		{
			val.set(5,0.0);
		}
		if(k!=0)
		{
			val.set(6,val.get(6)/k);
		}
		else
		{
			val.set(6,0.0);
		}
		if(val.get(14)!=0)
		{	
		BUSS_DELAY=val.get(8)/val.get(14);
		}
		else
		{
		BUSS_DELAY=val.get(8);	
		}
		sb.append("TCP_ATT_CNT:").append(val.get(0)).append("|");
		sb.append("TCP_SUCC_CNT:").append(val.get(1)).append("|");
		sb.append("TCP_SYNACK23_CNT:").append(val.get(2)).append("|");
		sb.append("TCP_SYNACK12_CNT:").append(val.get(3)).append("|");
		sb.append("TCP_RESPONSE_DELAY:").append(val.get(4)).append("|");
		sb.append("TCP_CONFIRM_DELAY:").append(val.get(5)).append("|");
		sb.append("FIRST_HTTP_RES_DELAY:").append(val.get(6)).append("|");
		sb.append("HTTP_RSP_FAIL_CNT:").append(val.get(7)).append("|");
		sb.append("BUSS_DUR:").append(val.get(8)).append("|");
		sb.append("HTTP_ATT_CNT:").append(val.get(9)).append("|");
		sb.append("DL_DATA:").append(val.get(10)).append("|");
		sb.append("SUCC_PAGEDISPLAY_CNT:").append(val.get(11)).append("|");
		sb.append("UL_DATA:").append(val.get(12)).append("|");
		sb.append("BUSS_SUCC_CNT:").append(val.get(13)).append("|");
		sb.append("BUSS_ATT_CNT:").append(val.get(14)).append("|");
		sb.append("BUSS_DELAY:").append(BUSS_DELAY).append("|");
		sb.append("HTTP_SUCC_CNT:").append(val.get(15)).append("|");
		sb.append("HTTP_FAIL_CNT:").append(val.get(16)).append("|");
		log.info("汇总后的记录："+sb.toString());
		filter_return.add(sb.toString());
		
		}
		return filter_return;
	}
	/**
	 * HTTP的用户粒度指标分组求和
	 * 
	 * @param tab
	 *            表名
	 * @param msisdns
	 *            手机号码，多个用,分割
	 * @param startTime
	 *            结束时间开始点
	 * @param endTime
	 *            结束时间结束点
	 * @return 用户粒度指标汇总数据
	 */
	public List<String> httpUserFilterGroup(String msisdn, String startTime, String endTime,String filed) {
		// 查询详单结果
		List<String> list = getList(103,msisdn, startTime, endTime);
		log.info("httpUserFilterGroup :list的大小："+list.size());
		log.info("数据："+list.get(0));
		if (list.size() == 0) {
			list.add(0, "0");
			log.info("httpUserFilterGroup :list的大小为0："+list.size());
			return list;
		}
		int size = list.size();
		String DAY_ID = null;
		String HOUR_ID = null;
		String IMSI = null;
		String MSISDN= null;
		String  tcp_conn_status=null;
		String APP_DESC= null;
		String SUBAPP_DESC= null;
		long HTTP_WAP_STATUS;
		double TCP_ATT_CNT=0;
		double TCP_SUCC_CNT=0;
		double 	TCP_SYNACK23_CNT=0;
		double 	TCP_SYNACK12_CNT=0;
		double 	TCP_RESPONSE_DELAY=0;
		double 	TCP_CONFIRM_DELAY=0;
		double 	FIRST_HTTP_RES_DELAY=0;
		double 	HTTP_RSP_FAIL_CNT=0;
		double 	BUSS_DUR=0;
		double 	HTTP_ATT_CNT=0;
		double 	DL_DATA=0;
		double 	UL_DATA=0;
		double 	SUCC_PAGEDISPLAY_CNT=0;
		double CONTENT_LENGTH=0;
		double BUSS_SUCC_CNT=0;//新加
		double BUSS_ATT_CNT=0;//新加
		double BUSS_DELAY=0;//新加
		double HTTP_SUCC_CNT=0;//新加
		double HTTP_FAIL_CNT=0;//新加
		boolean filter_flag=false;
		Map<String,List<Double>> tMap = new HashMap<String,List<Double>>();
		Map<Integer,String> key_map = new HashMap<Integer,String>();
		List<String> filter_return = new ArrayList<String>();
		String[] keypair_value=null;
		Integer feild_seq=0;
		int i=0,j=0,k=0;
		try {
		if(filed!=null&&!"null".equals(filed))
		{
			filter_flag=true;
		    keypair_value =filed.split("\\:", -1);
		    feild_seq=feild_table.get(keypair_value[0]);
//			String[] keypair = filed.split("\\,", -1);
//			for (String key : keypair) {
//				String[] keypair_value =key.split("\\:", -1);
//				Integer feild_seq=feild_table.get(keypair_value[0]);
//				key_map.put(feild_seq, keypair_value[1]);
//			}
		}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		for (String lines : list) {
			boolean eq_flag=true;
			List<Double> filter = new ArrayList<Double>();
			String[] line = lines.split("\\|", -1);
			String keyGroupBy="-1";
			//log.info(lines);
			//log.info("HTTP_WAP_STATUS:"+line[98]+"tcp_conn_status:"+line[94]+"TCP_RESPONSE_DELAY: "+line[87]+"TCP_CONFIRM_DELAY:"+line[88]
			//		+"FIRST_HTTP_RES_DELAY:"+line[101]);
			try {
				DAY_ID = line[0];
				HOUR_ID =line[1];
				IMSI=line[9];
				MSISDN=line[11];
				APP_DESC=line[49];
				SUBAPP_DESC=line[51];
				if(DAY_ID==null||HOUR_ID==null||IMSI==null||MSISDN==null)
				{
					continue;
				}
				
				if(filter_flag&&!line[feild_seq].equals(keypair_value[1]))
				{
					continue;
				}
				keyGroupBy="DAY_ID:"+DAY_ID+"|"+"HOUR_ID:"+HOUR_ID;
				try {
				    HTTP_WAP_STATUS = Long.parseLong(addDefault(line[98], "0"));
				    CONTENT_LENGTH=Double.parseDouble(addDefault(line[111].trim(), "0"));
				} catch (NumberFormatException e1) {
				    HTTP_WAP_STATUS=-1;
				    CONTENT_LENGTH=0;
				}
				tcp_conn_status= addDefault(line[94].trim(), "0");
				BUSS_ATT_CNT=1;
				if(line[93]!=null)
				 	TCP_ATT_CNT=Double.parseDouble(line[93]);
				if (tcp_conn_status.equals("0")) {
					TCP_SUCC_CNT = TCP_ATT_CNT;
					BUSS_SUCC_CNT=1;
				}
				if(HTTP_WAP_STATUS>=300){
					HTTP_RSP_FAIL_CNT=1;
				   }
			    
				if(line[87]!=null)
				{
					i++;
					TCP_RESPONSE_DELAY=Double.parseDouble(addDefault(line[87].trim(), "0"));
				}
				if(line[88]!=null)
				{
					j++;
					TCP_CONFIRM_DELAY=Double.parseDouble(addDefault(line[88].trim(), "0"));
				}
				if(line[101]!=null)
				{
					k++;
					FIRST_HTTP_RES_DELAY=Double.parseDouble(addDefault(line[101].trim(), "0"));
				}
				UL_DATA=Double.parseDouble(addDefault(line[77].trim(), "0"));
				DL_DATA=Double.parseDouble(addDefault(line[78].trim(), "0"));
				HTTP_ATT_CNT=1;
				if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
					if((UL_DATA+DL_DATA)>CONTENT_LENGTH){
						SUCC_PAGEDISPLAY_CNT=1;  
					}
				    }
				if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
					HTTP_SUCC_CNT=1;
				    }
				else
				{
					HTTP_FAIL_CNT=1;
				}
				if(TCP_CONFIRM_DELAY>0){
				    	TCP_SYNACK12_CNT =1; 
					   }
					if(TCP_RESPONSE_DELAY>0){
					    TCP_SYNACK23_CNT =1; 
					}
				 	
				BUSS_DUR=Long.parseLong(line[60].trim())- Long.parseLong(line[59].trim()) ;
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			filter.add(TCP_ATT_CNT);
			filter.add(TCP_SUCC_CNT);
			filter.add(TCP_SYNACK23_CNT);
			filter.add(TCP_SYNACK12_CNT);
			filter.add(TCP_RESPONSE_DELAY);
			filter.add(TCP_CONFIRM_DELAY);
			filter.add(FIRST_HTTP_RES_DELAY);
			filter.add(HTTP_RSP_FAIL_CNT);
			filter.add(BUSS_DUR);
			filter.add(HTTP_ATT_CNT);
			filter.add(DL_DATA);
			filter.add(SUCC_PAGEDISPLAY_CNT);
			filter.add(UL_DATA);
			filter.add(BUSS_SUCC_CNT);
			filter.add(BUSS_ATT_CNT);
			filter.add(HTTP_SUCC_CNT);
			filter.add(HTTP_FAIL_CNT);
			if(tMap.containsKey(keyGroupBy)){
				tMap.get(keyGroupBy).set(0,(tMap.get(keyGroupBy).get(0)+TCP_ATT_CNT));
				tMap.get(keyGroupBy).set(1,(tMap.get(keyGroupBy).get(1)+TCP_SUCC_CNT));
				tMap.get(keyGroupBy).set(2,(tMap.get(keyGroupBy).get(2)+TCP_SYNACK23_CNT));
				tMap.get(keyGroupBy).set(3,(tMap.get(keyGroupBy).get(3)+TCP_SYNACK12_CNT));
				tMap.get(keyGroupBy).set(4,(tMap.get(keyGroupBy).get(4)+TCP_RESPONSE_DELAY));
				tMap.get(keyGroupBy).set(5,(tMap.get(keyGroupBy).get(5)+TCP_CONFIRM_DELAY));
				tMap.get(keyGroupBy).set(6,(tMap.get(keyGroupBy).get(6)+FIRST_HTTP_RES_DELAY));
				tMap.get(keyGroupBy).set(7,(tMap.get(keyGroupBy).get(7)+HTTP_RSP_FAIL_CNT));
				tMap.get(keyGroupBy).set(8,(tMap.get(keyGroupBy).get(8)+BUSS_DUR));
				tMap.get(keyGroupBy).set(9,(tMap.get(keyGroupBy).get(9)+HTTP_ATT_CNT));
				tMap.get(keyGroupBy).set(10,(tMap.get(keyGroupBy).get(10)+DL_DATA));
				tMap.get(keyGroupBy).set(11,(tMap.get(keyGroupBy).get(11)+SUCC_PAGEDISPLAY_CNT));
				tMap.get(keyGroupBy).set(12,(tMap.get(keyGroupBy).get(12)+UL_DATA));
				tMap.get(keyGroupBy).set(13,(tMap.get(keyGroupBy).get(13)+BUSS_SUCC_CNT));
				tMap.get(keyGroupBy).set(14,(tMap.get(keyGroupBy).get(14)+BUSS_ATT_CNT));
				tMap.get(keyGroupBy).set(15,(tMap.get(keyGroupBy).get(15)+HTTP_SUCC_CNT));
				tMap.get(keyGroupBy).set(16,(tMap.get(keyGroupBy).get(16)+HTTP_FAIL_CNT));
				
			
			}else{
				tMap.put(keyGroupBy,filter);
			}

		}
		Iterator iter = tMap.entrySet().iterator();
		while (iter.hasNext()) {
		Map.Entry entry = (Map.Entry) iter.next();
		Object key = entry.getKey();
		List<Double> val = (List<Double>)entry.getValue();
		StringBuffer sb = new StringBuffer();
		sb.append(key.toString()).append("|");
		if(i!=0)
		{
			val.set(4,val.get(4)/i);
		}
		else
		{
			val.set(4,0.0);
		}
		if(j!=0)
		{
			val.set(5,val.get(5)/j);
		}
		else
		{
			val.set(5,0.0);
		}
		if(k!=0)
		{
			val.set(6,val.get(6)/k);
		}
		else
		{
			val.set(6,0.0);
		}
		if(val.get(14)!=0)
		{	
		BUSS_DELAY=val.get(8)/val.get(14);
		}
		else
		{
		BUSS_DELAY=val.get(8);	
		}
		sb.append("TCP_ATT_CNT:").append(val.get(0)).append("|");
		sb.append("TCP_SUCC_CNT:").append(val.get(1)).append("|");
		sb.append("TCP_SYNACK23_CNT:").append(val.get(2)).append("|");
		sb.append("TCP_SYNACK12_CNT:").append(val.get(3)).append("|");
		sb.append("TCP_RESPONSE_DELAY:").append(val.get(4)).append("|");
		sb.append("TCP_CONFIRM_DELAY:").append(val.get(5)).append("|");
		sb.append("FIRST_HTTP_RES_DELAY:").append(val.get(6)).append("|");
		sb.append("HTTP_RSP_FAIL_CNT:").append(val.get(7)).append("|");
		sb.append("BUSS_DUR:").append(val.get(8)).append("|");
		sb.append("HTTP_ATT_CNT:").append(val.get(9)).append("|");
		sb.append("DL_DATA:").append(val.get(10)).append("|");
		sb.append("SUCC_PAGEDISPLAY_CNT:").append(val.get(11)).append("|");
		sb.append("UL_DATA:").append(val.get(12)).append("|");
		sb.append("BUSS_SUCC_CNT:").append(val.get(13)).append("|");
		sb.append("BUSS_ATT_CNT:").append(val.get(14)).append("|");
		sb.append("BUSS_DELAY:").append(BUSS_DELAY).append("|");
		sb.append("HTTP_SUCC_CNT:").append(val.get(15)).append("|");
		sb.append("HTTP_FAIL_CNT:").append(val.get(16)).append("|");
		log.info("汇总后的记录："+sb.toString());
		filter_return.add(sb.toString());
		
		}
		return filter_return;
	}
	/**
	 * HTTP的用户终端,业务粒度原因码指标分组求和
	 * 
	 * @param tab
	 *            表名
	 * @param msisdns
	 *            手机号码，多个用,分割
	 * @param startTime
	 *            结束时间开始点
	 * @param endTime
	 *            结束时间结束点
	 * @return 用户,终端，业务粒度原因码指标汇总数据
	 */
	public List<String> httpUserTermSvCauseGroup(String msisdn, String startTime, String endTime,String cause) {
		// 查询详单结果
		List<String> list = getList(103,msisdn, startTime, endTime);
		log.info("list的大小："+list.size());

		if (list.size() == 0) {
			list.add(0, "0");
			log.info("list的大小为0："+list.size());
			return list;
		}
		int size = list.size();
		String DAY_ID = null;
		String HOUR_ID = null;
		String IMSI = null;
		String MSISDN= null;
		String IMEI_TAC= null;
		String TYPE_DESC= null;
		String APP_DESC= null;
		String SUBAPP_DESC= null;
		String  tcp_conn_status=null;
		long HTTP_WAP_STATUS;
		double TCP_ATT_CNT=0;
		double TCP_SUCC_CNT=0;
		double 	TCP_SYNACK23_CNT=0;
		double 	TCP_SYNACK12_CNT=0;
		double 	TCP_RESPONSE_DELAY=0;
		double 	TCP_CONFIRM_DELAY=0;
		double 	FIRST_HTTP_RES_DELAY=0;
		double 	HTTP_RSP_FAIL_CNT=0;
		double 	BUSS_DUR=0;
		double 	HTTP_ATT_CNT=0;
		double 	DL_DATA=0;
		double 	UL_DATA=0;
		double 	SUCC_PAGEDISPLAY_CNT=0;
		double CONTENT_LENGTH=0;
		double BUSS_SUCC_CNT=0;//新加
		double BUSS_ATT_CNT=0;//新加
		double BUSS_DELAY=0;//新加
		double HTTP_SUCC_CNT=0;//新加
		double HTTP_FAIL_CNT=0;//新加
		String[] cause_list = cause.split("\\,", -1);
		Map<String,List<Double>> tMap = new HashMap<String,List<Double>>();
		List<String> filter_return = new ArrayList<String>();
		int i=0,j=0,k=0;
		for (String lines : list) {
			boolean flag=true;
			List<Double> filter = new ArrayList<Double>();
			String[] line = lines.split("\\|", -1);
			String keyGroupBy="-1";
			
			try {
				DAY_ID = line[0];
				HOUR_ID =line[1];
				IMSI=line[9];
				MSISDN=line[11];
				IMEI_TAC=line[12];
				TYPE_DESC=line[14];
				APP_DESC=line[49];
				SUBAPP_DESC=line[51];
				if(DAY_ID==null||HOUR_ID==null||IMSI==null||MSISDN==null||IMEI_TAC==null||TYPE_DESC==null)
				{
					continue;
				}
				keyGroupBy="DAY_ID:"+DAY_ID+"|"+"HOUR_ID:"+HOUR_ID+"|"+"IMSI:"+IMSI+"|"+"MSISDN:"+MSISDN+"|"
						+"IMEI_TAC:"+IMEI_TAC+"|"+"TYPE_DESC:"+TYPE_DESC+"|"+"APP_DESC:"+APP_DESC+"|"+"SUBAPP_DESC:"+SUBAPP_DESC;
				//keyGroupBy=DAY_ID+"|"+HOUR_ID+"|"+IMSI+"|"+MSISDN+"|"+IMEI_TAC+"|"+TYPE_DESC+"|"+APP_DESC+"|"+SUBAPP_DESC;
				try {
				    HTTP_WAP_STATUS = Long.parseLong(addDefault(line[98], "0"));
				    CONTENT_LENGTH=Double.parseDouble(addDefault(line[111].trim(), "0"));
				} catch (NumberFormatException e1) {
				    HTTP_WAP_STATUS=-1;
				    CONTENT_LENGTH=0;
				}
				//是否在原因码里面
				for (String cause_temp : cause_list)
				{
				
				if(String.valueOf(HTTP_WAP_STATUS).equals(cause_temp))
				{
					flag=false;
					log.info("存在相同原因码不能跳过，原因码HTTP_WAP_STATUS："+HTTP_WAP_STATUS);
					break;
				}
				}
				if(flag)
				{
					continue;
				}
				tcp_conn_status= addDefault(line[94].trim(), "0");
				BUSS_ATT_CNT=1;
				if(line[93]!=null)
				 	TCP_ATT_CNT=Double.parseDouble(line[93]);
				if (tcp_conn_status.equals("0")) {
					TCP_SUCC_CNT = TCP_ATT_CNT;
					BUSS_SUCC_CNT=1;
				}
				if(HTTP_WAP_STATUS>=300){
					HTTP_RSP_FAIL_CNT=1;
				   }
			    
				if(line[87]!=null)
				{
					i++;
					TCP_RESPONSE_DELAY=Double.parseDouble(addDefault(line[87].trim(), "0"));
				}
				if(line[88]!=null)
				{
					j++;
					TCP_CONFIRM_DELAY=Double.parseDouble(addDefault(line[88].trim(), "0"));
				}
				if(line[101]!=null)
				{
					k++;
					FIRST_HTTP_RES_DELAY=Double.parseDouble(addDefault(line[101].trim(), "0"));
				}
				UL_DATA=Double.parseDouble(addDefault(line[77].trim(), "0"));
				DL_DATA=Double.parseDouble(addDefault(line[78].trim(), "0"));
				HTTP_ATT_CNT=1;
				if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
					if((UL_DATA+DL_DATA)>CONTENT_LENGTH){
						SUCC_PAGEDISPLAY_CNT=1;  
					}
				    }
				if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
					HTTP_SUCC_CNT=1;
				    }
				else
				{
					HTTP_FAIL_CNT=1;
				}
				if(TCP_CONFIRM_DELAY>0){
				    	TCP_SYNACK12_CNT =1; 
					   }
					if(TCP_RESPONSE_DELAY>0){
					    TCP_SYNACK23_CNT =1; 
					}
				 	
				BUSS_DUR=Long.parseLong(line[60].trim())- Long.parseLong(line[59].trim()) ;
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			filter.add(TCP_ATT_CNT);
			filter.add(TCP_SUCC_CNT);
			filter.add(TCP_SYNACK23_CNT);
			filter.add(TCP_SYNACK12_CNT);
			filter.add(TCP_RESPONSE_DELAY);
			filter.add(TCP_CONFIRM_DELAY);
			filter.add(FIRST_HTTP_RES_DELAY);
			filter.add(HTTP_RSP_FAIL_CNT);
			filter.add(BUSS_DUR);
			filter.add(HTTP_ATT_CNT);
			filter.add(DL_DATA);
			filter.add(SUCC_PAGEDISPLAY_CNT);
			filter.add(UL_DATA);
			filter.add(BUSS_SUCC_CNT);
			filter.add(BUSS_ATT_CNT);
			filter.add(HTTP_SUCC_CNT);
			filter.add(HTTP_FAIL_CNT);
			if(tMap.containsKey(keyGroupBy)){
				tMap.get(keyGroupBy).set(0,(tMap.get(keyGroupBy).get(0)+TCP_ATT_CNT));
				tMap.get(keyGroupBy).set(1,(tMap.get(keyGroupBy).get(1)+TCP_SUCC_CNT));
				tMap.get(keyGroupBy).set(2,(tMap.get(keyGroupBy).get(2)+TCP_SYNACK23_CNT));
				tMap.get(keyGroupBy).set(3,(tMap.get(keyGroupBy).get(3)+TCP_SYNACK12_CNT));
				tMap.get(keyGroupBy).set(4,(tMap.get(keyGroupBy).get(4)+TCP_RESPONSE_DELAY));
				tMap.get(keyGroupBy).set(5,(tMap.get(keyGroupBy).get(5)+TCP_CONFIRM_DELAY));
				tMap.get(keyGroupBy).set(6,(tMap.get(keyGroupBy).get(6)+FIRST_HTTP_RES_DELAY));
				tMap.get(keyGroupBy).set(7,(tMap.get(keyGroupBy).get(7)+HTTP_RSP_FAIL_CNT));
				tMap.get(keyGroupBy).set(8,(tMap.get(keyGroupBy).get(8)+BUSS_DUR));
				tMap.get(keyGroupBy).set(9,(tMap.get(keyGroupBy).get(9)+HTTP_ATT_CNT));
				tMap.get(keyGroupBy).set(10,(tMap.get(keyGroupBy).get(10)+DL_DATA));
				tMap.get(keyGroupBy).set(11,(tMap.get(keyGroupBy).get(11)+SUCC_PAGEDISPLAY_CNT));
				tMap.get(keyGroupBy).set(12,(tMap.get(keyGroupBy).get(12)+UL_DATA));
				tMap.get(keyGroupBy).set(13,(tMap.get(keyGroupBy).get(13)+BUSS_SUCC_CNT));
				tMap.get(keyGroupBy).set(14,(tMap.get(keyGroupBy).get(14)+BUSS_ATT_CNT));
				tMap.get(keyGroupBy).set(15,(tMap.get(keyGroupBy).get(15)+HTTP_SUCC_CNT));
				tMap.get(keyGroupBy).set(16,(tMap.get(keyGroupBy).get(16)+HTTP_FAIL_CNT));
				
			
			}else{
				tMap.put(keyGroupBy,filter);
			}

		}
		Iterator iter = tMap.entrySet().iterator();
		while (iter.hasNext()) {
		Map.Entry entry = (Map.Entry) iter.next();
		Object key = entry.getKey();
		List<Double> val = (List<Double>)entry.getValue();
		StringBuffer sb = new StringBuffer();
		sb.append(key.toString()).append("|");
		if(i!=0)
		{
			val.set(4,val.get(4)/i);
		}
		else
		{
			val.set(4,0.0);
		}
		if(j!=0)
		{
			val.set(5,val.get(5)/j);
		}
		else
		{
			val.set(5,0.0);
		}
		if(k!=0)
		{
			val.set(6,val.get(6)/k);
		}
		else
		{
			val.set(6,0.0);
		}
		if(val.get(14)!=0)
		{	
		BUSS_DELAY=val.get(8)/val.get(14);
		}
		else
		{
		BUSS_DELAY=val.get(8);	
		}
		sb.append("TCP_ATT_CNT:").append(val.get(0)).append("|");
		sb.append("TCP_SUCC_CNT:").append(val.get(1)).append("|");
		sb.append("TCP_SYNACK23_CNT:").append(val.get(2)).append("|");
		sb.append("TCP_SYNACK12_CNT:").append(val.get(3)).append("|");
		sb.append("TCP_RESPONSE_DELAY:").append(val.get(4)).append("|");
		sb.append("TCP_CONFIRM_DELAY:").append(val.get(5)).append("|");
		sb.append("FIRST_HTTP_RES_DELAY:").append(val.get(6)).append("|");
		sb.append("HTTP_RSP_FAIL_CNT:").append(val.get(7)).append("|");
		sb.append("BUSS_DUR:").append(val.get(8)).append("|");
		sb.append("HTTP_ATT_CNT:").append(val.get(9)).append("|");
		sb.append("DL_DATA:").append(val.get(10)).append("|");
		sb.append("SUCC_PAGEDISPLAY_CNT:").append(val.get(11)).append("|");
		sb.append("UL_DATA:").append(val.get(12)).append("|");
		sb.append("BUSS_SUCC_CNT:").append(val.get(13)).append("|");
		sb.append("BUSS_ATT_CNT:").append(val.get(14)).append("|");
		sb.append("BUSS_DELAY:").append(BUSS_DELAY).append("|");
		sb.append("HTTP_SUCC_CNT:").append(val.get(15)).append("|");
		sb.append("HTTP_FAIL_CNT:").append(val.get(16)).append("|");
		log.info("汇总后的记录："+sb.toString());
		filter_return.add(sb.toString());
		
		}
		
		return filter_return;
	}
	
	// ------------------------------------------
	/**
	 * HTTP的用户终端,业务粒度指标分组求和
	 * 
	 * @param tab
	 *            表名
	 * @param msisdns
	 *            手机号码，多个用,分割
	 * @param startTime
	 *            结束时间开始点
	 * @param endTime
	 *            结束时间结束点
	 * @return 用户,终端，业务粒度指标汇总数据
	 */
	public List<String> httpUserTermSvGroup(String msisdn, String startTime, String endTime) {
		// 查询详单结果
		List<String> list = getList(103,msisdn, startTime, endTime);
		log.info("list的大小："+list.size());

		if (list.size() == 0) {
			list.add(0, "0");
			log.info("list的大小为0："+list.size());
			return list;
		}
		int size = list.size();
		String DAY_ID = null;
		String HOUR_ID = null;
		String IMSI = null;
		String MSISDN= null;
		String IMEI_TAC= null;
		String TYPE_DESC= null;
		String APP_DESC= null;
		String SUBAPP_DESC= null;
		String  tcp_conn_status=null;
		long HTTP_WAP_STATUS;
		double TCP_ATT_CNT=0;
		double TCP_SUCC_CNT=0;
		double 	TCP_SYNACK23_CNT=0;
		double 	TCP_SYNACK12_CNT=0;
		double 	TCP_RESPONSE_DELAY=0;
		double 	TCP_CONFIRM_DELAY=0;
		double 	FIRST_HTTP_RES_DELAY=0;
		double 	HTTP_RSP_FAIL_CNT=0;
		double 	BUSS_DUR=0;
		double 	HTTP_ATT_CNT=0;
		double 	DL_DATA=0;
		double 	UL_DATA=0;
		double 	SUCC_PAGEDISPLAY_CNT=0;
		double CONTENT_LENGTH=0;
		double BUSS_SUCC_CNT=0;//新加
		double BUSS_ATT_CNT=0;//新加
		double BUSS_DELAY=0;//新加
		double HTTP_SUCC_CNT=0;//新加
		double HTTP_FAIL_CNT=0;//新加
		Map<String,List<Double>> tMap = new HashMap<String,List<Double>>();
		List<String> filter_return = new ArrayList<String>();
		int i=0,j=0,k=0;
		for (String lines : list) {
			
			List<Double> filter = new ArrayList<Double>();
			String[] line = lines.split("\\|", -1);
			String keyGroupBy="-1";
			
			try {
				DAY_ID = line[0];
				HOUR_ID =line[1];
				IMSI=line[9];
				MSISDN=line[11];
				IMEI_TAC=line[12];
				TYPE_DESC=line[14];
				APP_DESC=line[49];
				SUBAPP_DESC=line[51];
				if(DAY_ID==null||HOUR_ID==null||IMSI==null||MSISDN==null||IMEI_TAC==null||TYPE_DESC==null)
				{
					continue;
				}
				keyGroupBy="DAY_ID:"+DAY_ID+"|"+"HOUR_ID:"+HOUR_ID+"|"+"IMSI:"+IMSI+"|"+"MSISDN:"+MSISDN+"|"
						+"IMEI_TAC:"+IMEI_TAC+"|"+"TYPE_DESC:"+TYPE_DESC+"|"+"APP_DESC:"+APP_DESC+"|"+"SUBAPP_DESC:"+SUBAPP_DESC;
				//keyGroupBy=DAY_ID+"|"+HOUR_ID+"|"+IMSI+"|"+MSISDN+"|"+IMEI_TAC+"|"+TYPE_DESC+"|"+APP_DESC+"|"+SUBAPP_DESC;
				try {
				    HTTP_WAP_STATUS = Long.parseLong(addDefault(line[98], "0"));
				    CONTENT_LENGTH=Double.parseDouble(addDefault(line[111].trim(), "0"));
				} catch (NumberFormatException e1) {
				    HTTP_WAP_STATUS=-1;
				    CONTENT_LENGTH=0;
				}
				tcp_conn_status= addDefault(line[94].trim(), "0");
				BUSS_ATT_CNT=1;
				if(line[93]!=null)
				 	TCP_ATT_CNT=Double.parseDouble(line[93]);
				if (tcp_conn_status.equals("0")) {
					TCP_SUCC_CNT = TCP_ATT_CNT;
					BUSS_SUCC_CNT=1;
				}
				if(HTTP_WAP_STATUS>=300){
					HTTP_RSP_FAIL_CNT=1;
				   }
			    
				if(line[87]!=null)
				{
					i++;
					TCP_RESPONSE_DELAY=Double.parseDouble(addDefault(line[87].trim(), "0"));
				}
				if(line[88]!=null)
				{
					j++;
					TCP_CONFIRM_DELAY=Double.parseDouble(addDefault(line[88].trim(), "0"));
				}
				if(line[101]!=null)
				{
					k++;
					FIRST_HTTP_RES_DELAY=Double.parseDouble(addDefault(line[101].trim(), "0"));
				}
				UL_DATA=Double.parseDouble(addDefault(line[77].trim(), "0"));
				DL_DATA=Double.parseDouble(addDefault(line[78].trim(), "0"));
				HTTP_ATT_CNT=1;
				if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
					if((UL_DATA+DL_DATA)>CONTENT_LENGTH){
						SUCC_PAGEDISPLAY_CNT=1;  
					}
				    }
				if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
					HTTP_SUCC_CNT=1;
				    }
				else
				{
					HTTP_FAIL_CNT=1;
				}
				if(TCP_CONFIRM_DELAY>0){
				    	TCP_SYNACK12_CNT =1; 
					   }
					if(TCP_RESPONSE_DELAY>0){
					    TCP_SYNACK23_CNT =1; 
					}
				 	
				BUSS_DUR=Long.parseLong(line[60].trim())- Long.parseLong(line[59].trim()) ;
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			filter.add(TCP_ATT_CNT);
			filter.add(TCP_SUCC_CNT);
			filter.add(TCP_SYNACK23_CNT);
			filter.add(TCP_SYNACK12_CNT);
			filter.add(TCP_RESPONSE_DELAY);
			filter.add(TCP_CONFIRM_DELAY);
			filter.add(FIRST_HTTP_RES_DELAY);
			filter.add(HTTP_RSP_FAIL_CNT);
			filter.add(BUSS_DUR);
			filter.add(HTTP_ATT_CNT);
			filter.add(DL_DATA);
			filter.add(SUCC_PAGEDISPLAY_CNT);
			filter.add(UL_DATA);
			filter.add(BUSS_SUCC_CNT);
			filter.add(BUSS_ATT_CNT);
			filter.add(HTTP_SUCC_CNT);
			filter.add(HTTP_FAIL_CNT);
			if(tMap.containsKey(keyGroupBy)){
				tMap.get(keyGroupBy).set(0,(tMap.get(keyGroupBy).get(0)+TCP_ATT_CNT));
				tMap.get(keyGroupBy).set(1,(tMap.get(keyGroupBy).get(1)+TCP_SUCC_CNT));
				tMap.get(keyGroupBy).set(2,(tMap.get(keyGroupBy).get(2)+TCP_SYNACK23_CNT));
				tMap.get(keyGroupBy).set(3,(tMap.get(keyGroupBy).get(3)+TCP_SYNACK12_CNT));
				tMap.get(keyGroupBy).set(4,(tMap.get(keyGroupBy).get(4)+TCP_RESPONSE_DELAY));
				tMap.get(keyGroupBy).set(5,(tMap.get(keyGroupBy).get(5)+TCP_CONFIRM_DELAY));
				tMap.get(keyGroupBy).set(6,(tMap.get(keyGroupBy).get(6)+FIRST_HTTP_RES_DELAY));
				tMap.get(keyGroupBy).set(7,(tMap.get(keyGroupBy).get(7)+HTTP_RSP_FAIL_CNT));
				tMap.get(keyGroupBy).set(8,(tMap.get(keyGroupBy).get(8)+BUSS_DUR));
				tMap.get(keyGroupBy).set(9,(tMap.get(keyGroupBy).get(9)+HTTP_ATT_CNT));
				tMap.get(keyGroupBy).set(10,(tMap.get(keyGroupBy).get(10)+DL_DATA));
				tMap.get(keyGroupBy).set(11,(tMap.get(keyGroupBy).get(11)+SUCC_PAGEDISPLAY_CNT));
				tMap.get(keyGroupBy).set(12,(tMap.get(keyGroupBy).get(12)+UL_DATA));
				tMap.get(keyGroupBy).set(13,(tMap.get(keyGroupBy).get(13)+BUSS_SUCC_CNT));
				tMap.get(keyGroupBy).set(14,(tMap.get(keyGroupBy).get(14)+BUSS_ATT_CNT));
				tMap.get(keyGroupBy).set(15,(tMap.get(keyGroupBy).get(15)+HTTP_SUCC_CNT));
				tMap.get(keyGroupBy).set(16,(tMap.get(keyGroupBy).get(16)+HTTP_FAIL_CNT));
				
			
			}else{
				tMap.put(keyGroupBy,filter);
			}

		}
		Iterator iter = tMap.entrySet().iterator();
		while (iter.hasNext()) {
		Map.Entry entry = (Map.Entry) iter.next();
		Object key = entry.getKey();
		List<Double> val = (List<Double>)entry.getValue();
		StringBuffer sb = new StringBuffer();
		sb.append(key.toString()).append("|");
		if(i!=0)
		{
			val.set(4,val.get(4)/i);
		}
		else
		{
			val.set(4,0.0);
		}
		if(j!=0)
		{
			val.set(5,val.get(5)/j);
		}
		else
		{
			val.set(5,0.0);
		}
		if(k!=0)
		{
			val.set(6,val.get(6)/k);
		}
		else
		{
			val.set(6,0.0);
		}
		if(val.get(14)!=0)
		{	
		BUSS_DELAY=val.get(8)/val.get(14);
		}
		else
		{
		BUSS_DELAY=val.get(8);	
		}
		sb.append("TCP_ATT_CNT:").append(val.get(0)).append("|");
		sb.append("TCP_SUCC_CNT:").append(val.get(1)).append("|");
		sb.append("TCP_SYNACK23_CNT:").append(val.get(2)).append("|");
		sb.append("TCP_SYNACK12_CNT:").append(val.get(3)).append("|");
		sb.append("TCP_RESPONSE_DELAY:").append(val.get(4)).append("|");
		sb.append("TCP_CONFIRM_DELAY:").append(val.get(5)).append("|");
		sb.append("FIRST_HTTP_RES_DELAY:").append(val.get(6)).append("|");
		sb.append("HTTP_RSP_FAIL_CNT:").append(val.get(7)).append("|");
		sb.append("BUSS_DUR:").append(val.get(8)).append("|");
		sb.append("HTTP_ATT_CNT:").append(val.get(9)).append("|");
		sb.append("DL_DATA:").append(val.get(10)).append("|");
		sb.append("SUCC_PAGEDISPLAY_CNT:").append(val.get(11)).append("|");
		sb.append("UL_DATA:").append(val.get(12)).append("|");
		sb.append("BUSS_SUCC_CNT:").append(val.get(13)).append("|");
		sb.append("BUSS_ATT_CNT:").append(val.get(14)).append("|");
		sb.append("BUSS_DELAY:").append(BUSS_DELAY).append("|");
		sb.append("HTTP_SUCC_CNT:").append(val.get(15)).append("|");
		sb.append("HTTP_FAIL_CNT:").append(val.get(16)).append("|");
		log.info("汇总后的记录："+sb.toString());
		filter_return.add(sb.toString());
		
		}
		return filter_return;
		
	}
	/**
	 * HTTP的用户终端,小区业务粒度指标分组求和
	 * 
	 * @param tab
	 *            表名
	 * @param msisdns
	 *            手机号码，多个用,分割
	 * @param startTime
	 *            结束时间开始点
	 * @param endTime
	 *            结束时间结束点
	 * @return 用户,终端，业务粒度指标汇总数据
	 */
	public List<String> httpUserTermCellSvGroup(String msisdn, String startTime, String endTime) {
		// 查询详单结果
		List<String> list = getList(103,msisdn, startTime, endTime);
		log.info("list的大小："+list.size());

		if (list.size() == 0) {
			list.add(0, "0");
			log.info("list的大小为0："+list.size());
			return list;
		}
		int size = list.size();
		String DAY_ID = null;
		String HOUR_ID = null;
		String IMSI = null;
		String MSISDN= null;
		String IMEI_TAC= null;
		String TYPE_DESC= null;
		String APP_DESC= null;
		String SUBAPP_DESC= null;
		String  tcp_conn_status=null;
		String TAC = null;
		String EC_ID= null;
		String EC_DESC=null;
		String IMEI=null;
		long HTTP_WAP_STATUS;
		double TCP_ATT_CNT=0;
		double TCP_SUCC_CNT=0;
		double 	TCP_SYNACK23_CNT=0;
		double 	TCP_SYNACK12_CNT=0;
		double 	TCP_RESPONSE_DELAY=0;
		double 	TCP_CONFIRM_DELAY=0;
		double 	FIRST_HTTP_RES_DELAY=0;
		double 	HTTP_RSP_FAIL_CNT=0;
		double 	BUSS_DUR=0;
		double 	HTTP_ATT_CNT=0;
		double 	DL_DATA=0;
		double 	UL_DATA=0;
		double 	SUCC_PAGEDISPLAY_CNT=0;
		double CONTENT_LENGTH=0;
		double BUSS_SUCC_CNT=0;//新加
		double BUSS_ATT_CNT=0;//新加
		double BUSS_DELAY=0;//新加
		double HTTP_SUCC_CNT=0;//新加
		double HTTP_FAIL_CNT=0;//新加
		Map<String,List<Double>> tMap = new HashMap<String,List<Double>>();
		List<String> filter_return = new ArrayList<String>();
		int i=0,j=0,k=0;
		for (String lines : list) {
			
			List<Double> filter = new ArrayList<Double>();
			String[] line = lines.split("\\|", -1);
			String keyGroupBy="-1";
			
			try {
				DAY_ID = line[0];
				HOUR_ID =line[1];
				IMSI=line[9];
				MSISDN=line[11];
				IMEI_TAC=line[12];
				TYPE_DESC=line[14];
				APP_DESC=line[49];
				SUBAPP_DESC=line[51];
				TAC=line[30];
				EC_ID=line[32];
				EC_DESC=line[33];
				IMEI=line[10];
				if(DAY_ID==null||IMSI==null||MSISDN==null||TYPE_DESC==null||TAC==null)
				{
					continue;
				}
				keyGroupBy="DAY_ID:"+DAY_ID+"|"+"IMSI:"+IMSI+"|"+"IMEI:"+IMEI+"|"
						+"TYPE_DESC:"+TYPE_DESC+"|"+"MSISDN:"+msisdn+"|"+"TAC:"+TAC
						+"|"+"EC_ID:"+EC_ID+"|"+"EC_DESC:"+EC_DESC+"|"+"APP_DESC:"+APP_DESC+"|"+"SUBAPP_DESC:"+SUBAPP_DESC;
				//keyGroupBy=DAY_ID+"|"+HOUR_ID+"|"+IMSI+"|"+MSISDN+"|"+IMEI_TAC+"|"+TYPE_DESC+"|"+APP_DESC+"|"+SUBAPP_DESC;
				try {
				    HTTP_WAP_STATUS = Long.parseLong(addDefault(line[98], "0"));
				    CONTENT_LENGTH=Double.parseDouble(addDefault(line[111].trim(), "0"));
				} catch (NumberFormatException e1) {
				    HTTP_WAP_STATUS=-1;
				    CONTENT_LENGTH=0;
				}
				tcp_conn_status= addDefault(line[94].trim(), "0");
				BUSS_ATT_CNT=1;
				if(line[93]!=null)
				 	TCP_ATT_CNT=Double.parseDouble(line[93]);
				if (tcp_conn_status.equals("0")) {
					TCP_SUCC_CNT = TCP_ATT_CNT;
					BUSS_SUCC_CNT=1;
				}
				if(HTTP_WAP_STATUS>=300){
					HTTP_RSP_FAIL_CNT=1;
				   }
			    
				if(line[87]!=null)
				{
					i++;
					TCP_RESPONSE_DELAY=Double.parseDouble(addDefault(line[87].trim(), "0"));
				}
				if(line[88]!=null)
				{
					j++;
					TCP_CONFIRM_DELAY=Double.parseDouble(addDefault(line[88].trim(), "0"));
				}
				if(line[101]!=null)
				{
					k++;
					FIRST_HTTP_RES_DELAY=Double.parseDouble(addDefault(line[101].trim(), "0"));
				}
				UL_DATA=Double.parseDouble(addDefault(line[77].trim(), "0"));
				DL_DATA=Double.parseDouble(addDefault(line[78].trim(), "0"));
				HTTP_ATT_CNT=1;
				if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
					if((UL_DATA+DL_DATA)>CONTENT_LENGTH){
						SUCC_PAGEDISPLAY_CNT=1;  
					}
				    }
				if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
					HTTP_SUCC_CNT=1;
				    }
				else
				{
					HTTP_FAIL_CNT=1;
				}
				if(TCP_CONFIRM_DELAY>0){
				    	TCP_SYNACK12_CNT =1; 
					   }
					if(TCP_RESPONSE_DELAY>0){
					    TCP_SYNACK23_CNT =1; 
					}
				 	
				BUSS_DUR=Long.parseLong(line[60].trim())- Long.parseLong(line[59].trim()) ;
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			filter.add(TCP_ATT_CNT);
			filter.add(TCP_SUCC_CNT);
			filter.add(TCP_SYNACK23_CNT);
			filter.add(TCP_SYNACK12_CNT);
			filter.add(TCP_RESPONSE_DELAY);
			filter.add(TCP_CONFIRM_DELAY);
			filter.add(FIRST_HTTP_RES_DELAY);
			filter.add(HTTP_RSP_FAIL_CNT);
			filter.add(BUSS_DUR);
			filter.add(HTTP_ATT_CNT);
			filter.add(DL_DATA);
			filter.add(SUCC_PAGEDISPLAY_CNT);
			filter.add(UL_DATA);
			filter.add(BUSS_SUCC_CNT);
			filter.add(BUSS_ATT_CNT);
			filter.add(HTTP_SUCC_CNT);
			filter.add(HTTP_FAIL_CNT);
			if(tMap.containsKey(keyGroupBy)){
				tMap.get(keyGroupBy).set(0,(tMap.get(keyGroupBy).get(0)+TCP_ATT_CNT));
				tMap.get(keyGroupBy).set(1,(tMap.get(keyGroupBy).get(1)+TCP_SUCC_CNT));
				tMap.get(keyGroupBy).set(2,(tMap.get(keyGroupBy).get(2)+TCP_SYNACK23_CNT));
				tMap.get(keyGroupBy).set(3,(tMap.get(keyGroupBy).get(3)+TCP_SYNACK12_CNT));
				tMap.get(keyGroupBy).set(4,(tMap.get(keyGroupBy).get(4)+TCP_RESPONSE_DELAY));
				tMap.get(keyGroupBy).set(5,(tMap.get(keyGroupBy).get(5)+TCP_CONFIRM_DELAY));
				tMap.get(keyGroupBy).set(6,(tMap.get(keyGroupBy).get(6)+FIRST_HTTP_RES_DELAY));
				tMap.get(keyGroupBy).set(7,(tMap.get(keyGroupBy).get(7)+HTTP_RSP_FAIL_CNT));
				tMap.get(keyGroupBy).set(8,(tMap.get(keyGroupBy).get(8)+BUSS_DUR));
				tMap.get(keyGroupBy).set(9,(tMap.get(keyGroupBy).get(9)+HTTP_ATT_CNT));
				tMap.get(keyGroupBy).set(10,(tMap.get(keyGroupBy).get(10)+DL_DATA));
				tMap.get(keyGroupBy).set(11,(tMap.get(keyGroupBy).get(11)+SUCC_PAGEDISPLAY_CNT));
				tMap.get(keyGroupBy).set(12,(tMap.get(keyGroupBy).get(12)+UL_DATA));
				tMap.get(keyGroupBy).set(13,(tMap.get(keyGroupBy).get(13)+BUSS_SUCC_CNT));
				tMap.get(keyGroupBy).set(14,(tMap.get(keyGroupBy).get(14)+BUSS_ATT_CNT));
				tMap.get(keyGroupBy).set(15,(tMap.get(keyGroupBy).get(15)+HTTP_SUCC_CNT));
				tMap.get(keyGroupBy).set(16,(tMap.get(keyGroupBy).get(16)+HTTP_FAIL_CNT));
				
			
			}else{
				tMap.put(keyGroupBy,filter);
			}

		}
		Iterator iter = tMap.entrySet().iterator();
		while (iter.hasNext()) {
		Map.Entry entry = (Map.Entry) iter.next();
		Object key = entry.getKey();
		List<Double> val = (List<Double>)entry.getValue();
		StringBuffer sb = new StringBuffer();
		sb.append(key.toString()).append("|");
		if(i!=0)
		{
			val.set(4,val.get(4)/i);
		}
		else
		{
			val.set(4,0.0);
		}
		if(j!=0)
		{
			val.set(5,val.get(5)/j);
		}
		else
		{
			val.set(5,0.0);
		}
		if(k!=0)
		{
			val.set(6,val.get(6)/k);
		}
		else
		{
			val.set(6,0.0);
		}
		if(val.get(14)!=0)
		{	
		BUSS_DELAY=val.get(8)/val.get(14);
		}
		else
		{
		BUSS_DELAY=val.get(8);	
		}
		sb.append("TCP_ATT_CNT:").append(val.get(0)).append("|");
		sb.append("TCP_SUCC_CNT:").append(val.get(1)).append("|");
		sb.append("TCP_SYNACK23_CNT:").append(val.get(2)).append("|");
		sb.append("TCP_SYNACK12_CNT:").append(val.get(3)).append("|");
		sb.append("TCP_RESPONSE_DELAY:").append(val.get(4)).append("|");
		sb.append("TCP_CONFIRM_DELAY:").append(val.get(5)).append("|");
		sb.append("FIRST_HTTP_RES_DELAY:").append(val.get(6)).append("|");
		sb.append("HTTP_RSP_FAIL_CNT:").append(val.get(7)).append("|");
		sb.append("BUSS_DUR:").append(val.get(8)).append("|");
		sb.append("HTTP_ATT_CNT:").append(val.get(9)).append("|");
		sb.append("DL_DATA:").append(val.get(10)).append("|");
		sb.append("SUCC_PAGEDISPLAY_CNT:").append(val.get(11)).append("|");
		sb.append("UL_DATA:").append(val.get(12)).append("|");
		sb.append("BUSS_SUCC_CNT:").append(val.get(13)).append("|");
		sb.append("BUSS_ATT_CNT:").append(val.get(14)).append("|");
		sb.append("BUSS_DELAY:").append(BUSS_DELAY).append("|");
		sb.append("HTTP_SUCC_CNT:").append(val.get(15)).append("|");
		sb.append("HTTP_FAIL_CNT:").append(val.get(16)).append("|");
		log.info("汇总后的记录："+sb.toString());
		filter_return.add(sb.toString());
		
		}
		return filter_return;
		
	}
	/**
	 * HTTP的用户原因码粒度指标分组求和
	 * 
	 * @param tab
	 *            表名
	 * @param msisdns
	 *            手机号码，多个用,分割
	 * @param startTime
	 *            结束时间开始点
	 * @param endTime
	 *            结束时间结束点
	 * @return 用户原因码标汇总数据
	 */
	public List<String> httpUserCauseGroup(String msisdn, String startTime, String endTime) {
		// 查询详单结果
		List<String> list = getList(103,msisdn, startTime, endTime);
		log.info("list的大小："+list.size());

		if (list.size() == 0) {
			list.add(0, "0");
			log.info("list的大小为0："+list.size());
			return list;
		}
		int size = list.size();
		String DAY_ID = null;
		String CAUSE_DESC = null;
		long HTTP_WAP_STATUS;
		double FAUSE_CNT=0;
		Map<String,List<Double>> tMap = new HashMap<String,List<Double>>();
		List<String> filter_return = new ArrayList<String>();
		int i=0,j=0,k=0;
		for (String lines : list) {
			
			List<Double> filter = new ArrayList<Double>();
			String[] line = lines.split("\\|", -1);
			String keyGroupBy="-1";
			
			try {
				DAY_ID = line[0];
				CAUSE_DESC=line[99];
				if(DAY_ID==null||CAUSE_DESC==null)
				{
					continue;
				}
				//keyGroupBy=DAY_ID+"|"+HOUR_ID+"|"+IMSI+"|"+MSISDN+"|"+IMEI_TAC+"|"+TYPE_DESC+"|"+APP_DESC+"|"+SUBAPP_DESC;
				try {
				    HTTP_WAP_STATUS = Long.parseLong(addDefault(line[98], "0"));
				 
				} catch (NumberFormatException e1) {
				    HTTP_WAP_STATUS=-1;

				}
				keyGroupBy="DAY_ID:"+DAY_ID+"|"+"HTTP_WAP_STATUS:"+HTTP_WAP_STATUS+"|"+"CAUSE_DESC:"+CAUSE_DESC;
				
				FAUSE_CNT=1;
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			filter.add(FAUSE_CNT);
			if(tMap.containsKey(keyGroupBy)){
				tMap.get(keyGroupBy).set(0,(tMap.get(keyGroupBy).get(0)+FAUSE_CNT));
			}else{
				tMap.put(keyGroupBy,filter);
			}

		}
		Iterator iter = tMap.entrySet().iterator();
		while (iter.hasNext()) {
		Map.Entry entry = (Map.Entry) iter.next();
		Object key = entry.getKey();
		List<Double> val = (List<Double>)entry.getValue();
		StringBuffer sb = new StringBuffer();
		sb.append(key.toString()).append("|");

		sb.append("FAUSE_CNT:").append(val.get(0)).append("|");
		log.info("汇总后的记录："+sb.toString());
		filter_return.add(sb.toString());
		
		}
		return filter_return;
		
	}
	/**
	 * HTTP的用户终端粒度指标分组求和
	 * 
	 * @param tab
	 *            表名
	 * @param msisdns
	 *            手机号码，多个用,分割
	 * @param startTime
	 *            结束时间开始点
	 * @param endTime
	 *            结束时间结束点
	 * @return 用户,终端粒度指标汇总数据
	 */
	public List<String> httpUserTermGroup(String msisdn, String startTime, String endTime) {
		// 查询详单结果
		List<String> list = getList(103,msisdn, startTime, endTime);
		log.info("list的大小："+list.size());

		if (list.size() == 0) {
			list.add(0, "0");
			log.info("list的大小为0："+list.size());
			return list;
		}
		int size = list.size();
		String DAY_ID = null;
		String HOUR_ID = null;
		String IMSI = null;
		String MSISDN= null;
		String IMEI_TAC= null;
		String TYPE_DESC= null;
		String  tcp_conn_status=null;
		long HTTP_WAP_STATUS;
		double TCP_ATT_CNT=0;
		double TCP_SUCC_CNT=0;
		double 	TCP_SYNACK23_CNT=0;
		double 	TCP_SYNACK12_CNT=0;
		double 	TCP_RESPONSE_DELAY=0;
		double 	TCP_CONFIRM_DELAY=0;
		double 	FIRST_HTTP_RES_DELAY=0;
		double 	HTTP_RSP_FAIL_CNT=0;
		double 	BUSS_DUR=0;
		double 	HTTP_ATT_CNT=0;
		double 	DL_DATA=0;
		double 	UL_DATA=0;
		double 	SUCC_PAGEDISPLAY_CNT=0;
		double CONTENT_LENGTH=0;
		double BUSS_SUCC_CNT=0;//新加
		double BUSS_ATT_CNT=0;//新加
		double BUSS_DELAY=0;//新加
		double HTTP_SUCC_CNT=0;//新加
		double HTTP_FAIL_CNT=0;//新加
		Map<String,List<Double>> tMap = new HashMap<String,List<Double>>();
		List<String> filter_return = new ArrayList<String>();
		int i=0,j=0,k=0;
		for (String lines : list) {
			
			List<Double> filter = new ArrayList<Double>();
			String[] line = lines.split("\\|", -1);
			String keyGroupBy="-1";
			
			try {
				DAY_ID = line[0];
				HOUR_ID =line[1];
				IMSI=line[9];
				MSISDN=line[11];
				IMEI_TAC=line[12];
				TYPE_DESC=line[14];
				if(DAY_ID==null||HOUR_ID==null||IMSI==null||MSISDN==null||IMEI_TAC==null||TYPE_DESC==null)
				{
					continue;
				}
				keyGroupBy="DAY_ID:"+DAY_ID+"|"+"HOUR_ID:"+HOUR_ID+"|"+"IMSI:"+IMSI+"|"+"MSISDN:"+MSISDN+"|"
						+"IMEI_TAC:"+IMEI_TAC+"|"+"TYPE_DESC:"+TYPE_DESC;
				//keyGroupBy=DAY_ID+"|"+HOUR_ID+"|"+IMSI+"|"+MSISDN+"|"+IMEI_TAC+"|"+TYPE_DESC;
				try {
				    HTTP_WAP_STATUS = Long.parseLong(addDefault(line[98], "0"));
				    CONTENT_LENGTH=Double.parseDouble(addDefault(line[111].trim(), "0"));
				} catch (NumberFormatException e1) {
				    HTTP_WAP_STATUS=-1;
				    CONTENT_LENGTH=0;
				}
				tcp_conn_status= addDefault(line[94].trim(), "0");
				BUSS_ATT_CNT=1;
				if(line[93]!=null)
				 	TCP_ATT_CNT=Double.parseDouble(line[93]);
				if (tcp_conn_status.equals("0")) {
					TCP_SUCC_CNT = TCP_ATT_CNT;
					BUSS_SUCC_CNT=1;
				}
				if(HTTP_WAP_STATUS>=300){
					HTTP_RSP_FAIL_CNT=1;
				   }
			    
				if(line[87]!=null)
				{
					i++;
					TCP_RESPONSE_DELAY=Double.parseDouble(addDefault(line[87].trim(), "0"));
				}
				if(line[88]!=null)
				{
					j++;
					TCP_CONFIRM_DELAY=Double.parseDouble(addDefault(line[88].trim(), "0"));
				}
				if(line[101]!=null)
				{
					k++;
					FIRST_HTTP_RES_DELAY=Double.parseDouble(addDefault(line[101].trim(), "0"));
				}
				UL_DATA=Double.parseDouble(addDefault(line[77].trim(), "0"));
				DL_DATA=Double.parseDouble(addDefault(line[78].trim(), "0"));
				HTTP_ATT_CNT=1;
				if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
					if((UL_DATA+DL_DATA)>CONTENT_LENGTH){
						SUCC_PAGEDISPLAY_CNT=1;  
					}
				    }
				if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
					HTTP_SUCC_CNT=1;
				    }
				else
				{
					HTTP_FAIL_CNT=1;
				}
				if(TCP_CONFIRM_DELAY>0){
				    	TCP_SYNACK12_CNT =1; 
					   }
					if(TCP_RESPONSE_DELAY>0){
					    TCP_SYNACK23_CNT =1; 
					}
				 	
				BUSS_DUR=Long.parseLong(line[60].trim())- Long.parseLong(line[59].trim()) ;
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			filter.add(TCP_ATT_CNT);
			filter.add(TCP_SUCC_CNT);
			filter.add(TCP_SYNACK23_CNT);
			filter.add(TCP_SYNACK12_CNT);
			filter.add(TCP_RESPONSE_DELAY);
			filter.add(TCP_CONFIRM_DELAY);
			filter.add(FIRST_HTTP_RES_DELAY);
			filter.add(HTTP_RSP_FAIL_CNT);
			filter.add(BUSS_DUR);
			filter.add(HTTP_ATT_CNT);
			filter.add(DL_DATA);
			filter.add(SUCC_PAGEDISPLAY_CNT);
			filter.add(UL_DATA);
			filter.add(BUSS_SUCC_CNT);
			filter.add(BUSS_ATT_CNT);
			filter.add(HTTP_SUCC_CNT);
			filter.add(HTTP_FAIL_CNT);
			if(tMap.containsKey(keyGroupBy)){
				tMap.get(keyGroupBy).set(0,(tMap.get(keyGroupBy).get(0)+TCP_ATT_CNT));
				tMap.get(keyGroupBy).set(1,(tMap.get(keyGroupBy).get(1)+TCP_SUCC_CNT));
				tMap.get(keyGroupBy).set(2,(tMap.get(keyGroupBy).get(2)+TCP_SYNACK23_CNT));
				tMap.get(keyGroupBy).set(3,(tMap.get(keyGroupBy).get(3)+TCP_SYNACK12_CNT));
				tMap.get(keyGroupBy).set(4,(tMap.get(keyGroupBy).get(4)+TCP_RESPONSE_DELAY));
				tMap.get(keyGroupBy).set(5,(tMap.get(keyGroupBy).get(5)+TCP_CONFIRM_DELAY));
				tMap.get(keyGroupBy).set(6,(tMap.get(keyGroupBy).get(6)+FIRST_HTTP_RES_DELAY));
				tMap.get(keyGroupBy).set(7,(tMap.get(keyGroupBy).get(7)+HTTP_RSP_FAIL_CNT));
				tMap.get(keyGroupBy).set(8,(tMap.get(keyGroupBy).get(8)+BUSS_DUR));
				tMap.get(keyGroupBy).set(9,(tMap.get(keyGroupBy).get(9)+HTTP_ATT_CNT));
				tMap.get(keyGroupBy).set(10,(tMap.get(keyGroupBy).get(10)+DL_DATA));
				tMap.get(keyGroupBy).set(11,(tMap.get(keyGroupBy).get(11)+SUCC_PAGEDISPLAY_CNT));
				tMap.get(keyGroupBy).set(12,(tMap.get(keyGroupBy).get(12)+UL_DATA));
				tMap.get(keyGroupBy).set(13,(tMap.get(keyGroupBy).get(13)+BUSS_SUCC_CNT));
				tMap.get(keyGroupBy).set(14,(tMap.get(keyGroupBy).get(14)+BUSS_ATT_CNT));
				tMap.get(keyGroupBy).set(15,(tMap.get(keyGroupBy).get(15)+HTTP_SUCC_CNT));
				tMap.get(keyGroupBy).set(16,(tMap.get(keyGroupBy).get(16)+HTTP_FAIL_CNT));
				
			
			}else{
				tMap.put(keyGroupBy,filter);
			}

		}
		Iterator iter = tMap.entrySet().iterator();
		while (iter.hasNext()) {
		Map.Entry entry = (Map.Entry) iter.next();
		Object key = entry.getKey();
		List<Double> val = (List<Double>)entry.getValue();
		StringBuffer sb = new StringBuffer();
		sb.append(key.toString()).append("|");
		if(i!=0)
		{
			val.set(4,val.get(4)/i);
		}
		else
		{
			val.set(4,0.0);
		}
		if(j!=0)
		{
			val.set(5,val.get(5)/j);
		}
		else
		{
			val.set(5,0.0);
		}
		if(k!=0)
		{
			val.set(6,val.get(6)/k);
		}
		else
		{
			val.set(6,0.0);
		}
		if(val.get(14)!=0)
		{	
		BUSS_DELAY=val.get(8)/val.get(14);
		}
		else
		{
		BUSS_DELAY=val.get(8);	
		}
		sb.append("TCP_ATT_CNT:").append(val.get(0)).append("|");
		sb.append("TCP_SUCC_CNT:").append(val.get(1)).append("|");
		sb.append("TCP_SYNACK23_CNT:").append(val.get(2)).append("|");
		sb.append("TCP_SYNACK12_CNT:").append(val.get(3)).append("|");
		sb.append("TCP_RESPONSE_DELAY:").append(val.get(4)).append("|");
		sb.append("TCP_CONFIRM_DELAY:").append(val.get(5)).append("|");
		sb.append("FIRST_HTTP_RES_DELAY:").append(val.get(6)).append("|");
		sb.append("HTTP_RSP_FAIL_CNT:").append(val.get(7)).append("|");
		sb.append("BUSS_DUR:").append(val.get(8)).append("|");
		sb.append("HTTP_ATT_CNT:").append(val.get(9)).append("|");
		sb.append("DL_DATA:").append(val.get(10)).append("|");
		sb.append("SUCC_PAGEDISPLAY_CNT:").append(val.get(11)).append("|");
		sb.append("UL_DATA:").append(val.get(12)).append("|");
		sb.append("BUSS_SUCC_CNT:").append(val.get(13)).append("|");
		sb.append("BUSS_ATT_CNT:").append(val.get(14)).append("|");
		sb.append("BUSS_DELAY:").append(BUSS_DELAY).append("|");
		sb.append("HTTP_SUCC_CNT:").append(val.get(15)).append("|");
		sb.append("HTTP_FAIL_CNT:").append(val.get(16)).append("|");
		log.info("汇总后的记录："+sb.toString());
		filter_return.add(sb.toString());
		
		}
		return filter_return;
	}
	/**
	 * HTTP的用户原因码粒度指标分组求和
	 * 
	 * @param tab
	 *            表名
	 * @param msisdns
	 *            手机号码，多个用,分割
	 * @param startTime
	 *            结束时间开始点
	 * @param endTime
	 *            结束时间结束点
	 * @return 用户原因码粒度指标汇总数据
	 */
	public List<String> httpUserFcauceGroup(String msisdn, String startTime, String endTime,String cause) {
		// 查询详单结果
		List<String> list = getList(103,msisdn, startTime, endTime);
		log.info("list的大小："+list.size());

		if (list.size() == 0) {
			list.add(0, "0");
			log.info("list的大小为0："+list.size());
			return list;
		}
		int size = list.size();
		String DAY_ID = null;
		String HOUR_ID = null;
		String IMSI = null;
		String MSISDN= null;
		String IMEI_TAC= null;
		String TYPE_DESC= null;
		String  tcp_conn_status=null;
		long HTTP_WAP_STATUS;
		double TCP_ATT_CNT=0;
		double TCP_SUCC_CNT=0;
		double 	TCP_SYNACK23_CNT=0;
		double 	TCP_SYNACK12_CNT=0;
		double 	TCP_RESPONSE_DELAY=0;
		double 	TCP_CONFIRM_DELAY=0;
		double 	FIRST_HTTP_RES_DELAY=0;
		double 	HTTP_RSP_FAIL_CNT=0;
		double 	BUSS_DUR=0;
		double 	HTTP_ATT_CNT=0;
		double 	DL_DATA=0;
		double 	UL_DATA=0;
		double 	SUCC_PAGEDISPLAY_CNT=0;
		double CONTENT_LENGTH=0;
		double BUSS_SUCC_CNT=0;//新加
		double BUSS_ATT_CNT=0;//新加
		double BUSS_DELAY=0;//新加
		double HTTP_SUCC_CNT=0;//新加
		double HTTP_FAIL_CNT=0;//新加
		String[] cause_list = cause.split("\\,", -1);
		Map<String,List<Double>> tMap = new HashMap<String,List<Double>>();
		List<String> filter_return = new ArrayList<String>();
		int i=0,j=0,k=0;
		
		for (String lines : list) {
			boolean flag=true;
			List<Double> filter = new ArrayList<Double>();
			String[] line = lines.split("\\|", -1);
			String keyGroupBy="-1";
			
			try {
				DAY_ID = line[0];
				HOUR_ID =line[1];
				IMSI=line[9];
				MSISDN=line[11];
				if(DAY_ID==null||HOUR_ID==null||IMSI==null||MSISDN==null)
				{
					continue;
				}
				keyGroupBy="DAY_ID:"+DAY_ID+"|"+"HOUR_ID:"+HOUR_ID+"|"+"IMSI:"+IMSI+"|"+"MSISDN:"+MSISDN;
				try {
				    HTTP_WAP_STATUS = Long.parseLong(addDefault(line[98], "0"));
				    CONTENT_LENGTH=Double.parseDouble(addDefault(line[111].trim(), "0"));
				} catch (NumberFormatException e1) {
				    HTTP_WAP_STATUS=-1;
				    CONTENT_LENGTH=0;
				}
				//是否在原因码里面
				for (String cause_temp : cause_list)
				{
				
				if(String.valueOf(HTTP_WAP_STATUS).equals(cause_temp))
				{
					flag=false;
					log.info("存在相同原因码不能跳过，原因码HTTP_WAP_STATUS："+HTTP_WAP_STATUS);
					break;
				}
				}
				if(flag)
				{
					continue;
				}
				
				tcp_conn_status= addDefault(line[94].trim(), "0");
				BUSS_ATT_CNT=1;
				if(line[93]!=null)
				 	TCP_ATT_CNT=Double.parseDouble(line[93]);
				if (tcp_conn_status.equals("0")) {
					TCP_SUCC_CNT = TCP_ATT_CNT;
					BUSS_SUCC_CNT=1;
				}
				if(HTTP_WAP_STATUS>=300){
					HTTP_RSP_FAIL_CNT=1;
				   }
			    
				if(line[87]!=null)
				{
					i++;
					TCP_RESPONSE_DELAY=Double.parseDouble(addDefault(line[87].trim(), "0"));
				}
				if(line[88]!=null)
				{
					j++;
					TCP_CONFIRM_DELAY=Double.parseDouble(addDefault(line[88].trim(), "0"));
				}
				if(line[101]!=null)
				{
					k++;
					FIRST_HTTP_RES_DELAY=Double.parseDouble(addDefault(line[101].trim(), "0"));
				}
				UL_DATA=Double.parseDouble(addDefault(line[77].trim(), "0"));
				DL_DATA=Double.parseDouble(addDefault(line[78].trim(), "0"));
				HTTP_ATT_CNT=1;
				if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
					if((UL_DATA+DL_DATA)>CONTENT_LENGTH){
						SUCC_PAGEDISPLAY_CNT=1;  
					}
				    }
				if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
					HTTP_SUCC_CNT=1;
				    }
				else
				{
					HTTP_FAIL_CNT=1;
				}
				if(TCP_CONFIRM_DELAY>0){
				    	TCP_SYNACK12_CNT =1; 
					   }
					if(TCP_RESPONSE_DELAY>0){
					    TCP_SYNACK23_CNT =1; 
					}
				 	
				BUSS_DUR=Long.parseLong(line[60].trim())- Long.parseLong(line[59].trim()) ;
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			filter.add(TCP_ATT_CNT);
			filter.add(TCP_SUCC_CNT);
			filter.add(TCP_SYNACK23_CNT);
			filter.add(TCP_SYNACK12_CNT);
			filter.add(TCP_RESPONSE_DELAY);
			filter.add(TCP_CONFIRM_DELAY);
			filter.add(FIRST_HTTP_RES_DELAY);
			filter.add(HTTP_RSP_FAIL_CNT);
			filter.add(BUSS_DUR);
			filter.add(HTTP_ATT_CNT);
			filter.add(DL_DATA);
			filter.add(SUCC_PAGEDISPLAY_CNT);
			filter.add(UL_DATA);
			filter.add(BUSS_SUCC_CNT);
			filter.add(BUSS_ATT_CNT);
			filter.add(HTTP_SUCC_CNT);
			filter.add(HTTP_FAIL_CNT);
			if(tMap.containsKey(keyGroupBy)){
				tMap.get(keyGroupBy).set(0,(tMap.get(keyGroupBy).get(0)+TCP_ATT_CNT));
				tMap.get(keyGroupBy).set(1,(tMap.get(keyGroupBy).get(1)+TCP_SUCC_CNT));
				tMap.get(keyGroupBy).set(2,(tMap.get(keyGroupBy).get(2)+TCP_SYNACK23_CNT));
				tMap.get(keyGroupBy).set(3,(tMap.get(keyGroupBy).get(3)+TCP_SYNACK12_CNT));
				tMap.get(keyGroupBy).set(4,(tMap.get(keyGroupBy).get(4)+TCP_RESPONSE_DELAY));
				tMap.get(keyGroupBy).set(5,(tMap.get(keyGroupBy).get(5)+TCP_CONFIRM_DELAY));
				tMap.get(keyGroupBy).set(6,(tMap.get(keyGroupBy).get(6)+FIRST_HTTP_RES_DELAY));
				tMap.get(keyGroupBy).set(7,(tMap.get(keyGroupBy).get(7)+HTTP_RSP_FAIL_CNT));
				tMap.get(keyGroupBy).set(8,(tMap.get(keyGroupBy).get(8)+BUSS_DUR));
				tMap.get(keyGroupBy).set(9,(tMap.get(keyGroupBy).get(9)+HTTP_ATT_CNT));
				tMap.get(keyGroupBy).set(10,(tMap.get(keyGroupBy).get(10)+DL_DATA));
				tMap.get(keyGroupBy).set(11,(tMap.get(keyGroupBy).get(11)+SUCC_PAGEDISPLAY_CNT));
				tMap.get(keyGroupBy).set(12,(tMap.get(keyGroupBy).get(12)+UL_DATA));
				tMap.get(keyGroupBy).set(13,(tMap.get(keyGroupBy).get(13)+BUSS_SUCC_CNT));
				tMap.get(keyGroupBy).set(14,(tMap.get(keyGroupBy).get(14)+BUSS_ATT_CNT));
				tMap.get(keyGroupBy).set(15,(tMap.get(keyGroupBy).get(15)+HTTP_SUCC_CNT));
				tMap.get(keyGroupBy).set(16,(tMap.get(keyGroupBy).get(16)+HTTP_FAIL_CNT));
				
			
			}else{
				tMap.put(keyGroupBy,filter);
			}

		}
		Iterator iter = tMap.entrySet().iterator();
		while (iter.hasNext()) {
		Map.Entry entry = (Map.Entry) iter.next();
		Object key = entry.getKey();
		List<Double> val = (List<Double>)entry.getValue();
		StringBuffer sb = new StringBuffer();
		sb.append(key.toString()).append("|");
		if(i!=0)
		{
			val.set(4,val.get(4)/i);
		}
		else
		{
			val.set(4,0.0);
		}
		if(j!=0)
		{
			val.set(5,val.get(5)/j);
		}
		else
		{
			val.set(5,0.0);
		}
		if(k!=0)
		{
			val.set(6,val.get(6)/k);
		}
		else
		{
			val.set(6,0.0);
		}
		if(val.get(14)!=0)
		{	
		BUSS_DELAY=val.get(8)/val.get(14);
		}
		else
		{
		BUSS_DELAY=val.get(8);	
		}
		sb.append("TCP_ATT_CNT:").append(val.get(0)).append("|");
		sb.append("TCP_SUCC_CNT:").append(val.get(1)).append("|");
		sb.append("TCP_SYNACK23_CNT:").append(val.get(2)).append("|");
		sb.append("TCP_SYNACK12_CNT:").append(val.get(3)).append("|");
		sb.append("TCP_RESPONSE_DELAY:").append(val.get(4)).append("|");
		sb.append("TCP_CONFIRM_DELAY:").append(val.get(5)).append("|");
		sb.append("FIRST_HTTP_RES_DELAY:").append(val.get(6)).append("|");
		sb.append("HTTP_RSP_FAIL_CNT:").append(val.get(7)).append("|");
		sb.append("BUSS_DUR:").append(val.get(8)).append("|");
		sb.append("HTTP_ATT_CNT:").append(val.get(9)).append("|");
		sb.append("DL_DATA:").append(val.get(10)).append("|");
		sb.append("SUCC_PAGEDISPLAY_CNT:").append(val.get(11)).append("|");
		sb.append("UL_DATA:").append(val.get(12)).append("|");
		sb.append("BUSS_SUCC_CNT:").append(val.get(13)).append("|");
		sb.append("BUSS_ATT_CNT:").append(val.get(14)).append("|");
		sb.append("BUSS_DELAY:").append(BUSS_DELAY).append("|");
		sb.append("HTTP_SUCC_CNT:").append(val.get(15)).append("|");
		sb.append("HTTP_FAIL_CNT:").append(val.get(16)).append("|");
		log.info("汇总后的记录："+sb.toString());
		filter_return.add(sb.toString());
		
		}
		return filter_return;
	}
	/**
	 * HTTP的小区业务分组求和
	 * 
	 * @param tab
	 *            表名
	 * @param msisdns
	 *            手机号码，多个用,分割
	 * @param startTime
	 *            结束时间开始点
	 * @param endTime
	 *            结束时间结束点
	 * @return 小区业务汇总数据
	 */
	public List<String> httpCellSvGroup(String msisdn, String startTime, String endTime) {
			// 查询详单结果
			List<String> list = getList(103,msisdn, startTime, endTime);
			log.info("list的大小："+list.size());

			if (list.size() == 0) {
				list.add(0, "0");
				log.info("list的大小为0："+list.size());
				return list;
			}
			int size = list.size();
			String DAY_ID = null;
			String HOUR_ID = null;
			String IMSI = null;
			String MSISDN= null;
			String IMEI_TAC= null;
			String TYPE_DESC= null;
			String APP_DESC= null;
			String SUBAPP_DESC= null;
			String CELL_DESC= null;
			String  tcp_conn_status=null;
			long HTTP_WAP_STATUS;
			double TCP_ATT_CNT=0;
			double TCP_SUCC_CNT=0;
			double 	TCP_SYNACK23_CNT=0;
			double 	TCP_SYNACK12_CNT=0;
			double 	TCP_RESPONSE_DELAY=0;
			double 	TCP_CONFIRM_DELAY=0;
			double 	FIRST_HTTP_RES_DELAY=0;
			double 	HTTP_RSP_FAIL_CNT=0;
			double 	BUSS_DUR=0;
			double 	HTTP_ATT_CNT=0;
			double 	DL_DATA=0;
			double 	UL_DATA=0;
			double 	SUCC_PAGEDISPLAY_CNT=0;
			double CONTENT_LENGTH=0;
			double BUSS_SUCC_CNT=0;//新加
			double BUSS_ATT_CNT=0;//新加
			double BUSS_DELAY=0;//新加
			double HTTP_SUCC_CNT=0;//新加
			double HTTP_FAIL_CNT=0;//新加
			Map<String,List<Double>> tMap = new HashMap<String,List<Double>>();
			List<String> filter_return = new ArrayList<String>();
			int i=0,j=0,k=0;
			for (String lines : list) {
				
				List<Double> filter = new ArrayList<Double>();
				String[] line = lines.split("\\|", -1);
				String keyGroupBy="-1";
				
				try {
					DAY_ID = line[0];
					HOUR_ID =line[1];
					IMSI=line[9];
					MSISDN=line[11];
					IMEI_TAC=line[12];
					TYPE_DESC=line[14];
					APP_DESC=line[49];
					SUBAPP_DESC=line[51];
					CELL_DESC=line[33];
					if(CELL_DESC==null||APP_DESC==null||SUBAPP_DESC==null)
					{
						continue;
					}
					keyGroupBy="EC_DESC:"+CELL_DESC+"|"+"APP_DESC:"+APP_DESC+"|"+"SUBAPP_DESC:"+SUBAPP_DESC;
					//keyGroupBy=DAY_ID+"|"+HOUR_ID+"|"+IMSI+"|"+MSISDN+"|"+IMEI_TAC+"|"+TYPE_DESC+"|"+APP_DESC+"|"+SUBAPP_DESC;
					try {
					    HTTP_WAP_STATUS = Long.parseLong(addDefault(line[98], "0"));
					    CONTENT_LENGTH=Double.parseDouble(addDefault(line[111].trim(), "0"));
					} catch (NumberFormatException e1) {
					    HTTP_WAP_STATUS=-1;
					    CONTENT_LENGTH=0;
					}
					tcp_conn_status= addDefault(line[94].trim(), "0");
					BUSS_ATT_CNT=1;
					if(line[93]!=null)
					 	TCP_ATT_CNT=Double.parseDouble(line[93]);
					if (tcp_conn_status.equals("0")) {
						TCP_SUCC_CNT = TCP_ATT_CNT;
						BUSS_SUCC_CNT=1;
					}
					if(HTTP_WAP_STATUS>=300){
						HTTP_RSP_FAIL_CNT=1;
					   }
				    
					if(line[87]!=null)
					{
						i++;
						TCP_RESPONSE_DELAY=Double.parseDouble(addDefault(line[87].trim(), "0"));
					}
					if(line[88]!=null)
					{
						j++;
						TCP_CONFIRM_DELAY=Double.parseDouble(addDefault(line[88].trim(), "0"));
					}
					if(line[101]!=null)
					{
						k++;
						FIRST_HTTP_RES_DELAY=Double.parseDouble(addDefault(line[101].trim(), "0"));
					}
					UL_DATA=Double.parseDouble(addDefault(line[77].trim(), "0"));
					DL_DATA=Double.parseDouble(addDefault(line[78].trim(), "0"));
					HTTP_ATT_CNT=1;
					if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
						if((UL_DATA+DL_DATA)>CONTENT_LENGTH){
							SUCC_PAGEDISPLAY_CNT=1;  
						}
					    }
					if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
						HTTP_SUCC_CNT=1;
					    }
					else
					{
						HTTP_FAIL_CNT=1;
					}
					if(TCP_CONFIRM_DELAY>0){
					    	TCP_SYNACK12_CNT =1; 
						   }
						if(TCP_RESPONSE_DELAY>0){
						    TCP_SYNACK23_CNT =1; 
						}
					 	
					BUSS_DUR=Long.parseLong(line[60].trim())- Long.parseLong(line[59].trim()) ;
					
				} catch (Exception e) {
					e.printStackTrace();
				}
				filter.add(TCP_ATT_CNT);
				filter.add(TCP_SUCC_CNT);
				filter.add(TCP_SYNACK23_CNT);
				filter.add(TCP_SYNACK12_CNT);
				filter.add(TCP_RESPONSE_DELAY);
				filter.add(TCP_CONFIRM_DELAY);
				filter.add(FIRST_HTTP_RES_DELAY);
				filter.add(HTTP_RSP_FAIL_CNT);
				filter.add(BUSS_DUR);
				filter.add(HTTP_ATT_CNT);
				filter.add(DL_DATA);
				filter.add(SUCC_PAGEDISPLAY_CNT);
				filter.add(UL_DATA);
				filter.add(BUSS_SUCC_CNT);
				filter.add(BUSS_ATT_CNT);
				filter.add(HTTP_SUCC_CNT);
				filter.add(HTTP_FAIL_CNT);
				if(tMap.containsKey(keyGroupBy)){
					tMap.get(keyGroupBy).set(0,(tMap.get(keyGroupBy).get(0)+TCP_ATT_CNT));
					tMap.get(keyGroupBy).set(1,(tMap.get(keyGroupBy).get(1)+TCP_SUCC_CNT));
					tMap.get(keyGroupBy).set(2,(tMap.get(keyGroupBy).get(2)+TCP_SYNACK23_CNT));
					tMap.get(keyGroupBy).set(3,(tMap.get(keyGroupBy).get(3)+TCP_SYNACK12_CNT));
					tMap.get(keyGroupBy).set(4,(tMap.get(keyGroupBy).get(4)+TCP_RESPONSE_DELAY));
					tMap.get(keyGroupBy).set(5,(tMap.get(keyGroupBy).get(5)+TCP_CONFIRM_DELAY));
					tMap.get(keyGroupBy).set(6,(tMap.get(keyGroupBy).get(6)+FIRST_HTTP_RES_DELAY));
					tMap.get(keyGroupBy).set(7,(tMap.get(keyGroupBy).get(7)+HTTP_RSP_FAIL_CNT));
					tMap.get(keyGroupBy).set(8,(tMap.get(keyGroupBy).get(8)+BUSS_DUR));
					tMap.get(keyGroupBy).set(9,(tMap.get(keyGroupBy).get(9)+HTTP_ATT_CNT));
					tMap.get(keyGroupBy).set(10,(tMap.get(keyGroupBy).get(10)+DL_DATA));
					tMap.get(keyGroupBy).set(11,(tMap.get(keyGroupBy).get(11)+SUCC_PAGEDISPLAY_CNT));
					tMap.get(keyGroupBy).set(12,(tMap.get(keyGroupBy).get(12)+UL_DATA));
					tMap.get(keyGroupBy).set(13,(tMap.get(keyGroupBy).get(13)+BUSS_SUCC_CNT));
					tMap.get(keyGroupBy).set(14,(tMap.get(keyGroupBy).get(14)+BUSS_ATT_CNT));
					tMap.get(keyGroupBy).set(15,(tMap.get(keyGroupBy).get(15)+HTTP_SUCC_CNT));
					tMap.get(keyGroupBy).set(16,(tMap.get(keyGroupBy).get(16)+HTTP_FAIL_CNT));
					
				
				}else{
					tMap.put(keyGroupBy,filter);
				}

			}
			Iterator iter = tMap.entrySet().iterator();
			while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			Object key = entry.getKey();
			List<Double> val = (List<Double>)entry.getValue();
			StringBuffer sb = new StringBuffer();
			sb.append(key.toString()).append("|");
			if(i!=0)
			{
				val.set(4,val.get(4)/i);
			}
			else
			{
				val.set(4,0.0);
			}
			if(j!=0)
			{
				val.set(5,val.get(5)/j);
			}
			else
			{
				val.set(5,0.0);
			}
			if(k!=0)
			{
				val.set(6,val.get(6)/k);
			}
			else
			{
				val.set(6,0.0);
			}
			if(val.get(14)!=0)
			{	
			BUSS_DELAY=val.get(8)/val.get(14);
			}
			else
			{
			BUSS_DELAY=val.get(8);	
			}
			sb.append("TCP_ATT_CNT:").append(val.get(0)).append("|");
			sb.append("TCP_SUCC_CNT:").append(val.get(1)).append("|");
			sb.append("TCP_SYNACK23_CNT:").append(val.get(2)).append("|");
			sb.append("TCP_SYNACK12_CNT:").append(val.get(3)).append("|");
			sb.append("TCP_RESPONSE_DELAY:").append(val.get(4)).append("|");
			sb.append("TCP_CONFIRM_DELAY:").append(val.get(5)).append("|");
			sb.append("FIRST_HTTP_RES_DELAY:").append(val.get(6)).append("|");
			sb.append("HTTP_RSP_FAIL_CNT:").append(val.get(7)).append("|");
			sb.append("BUSS_DUR:").append(val.get(8)).append("|");
			sb.append("HTTP_ATT_CNT:").append(val.get(9)).append("|");
			sb.append("DL_DATA:").append(val.get(10)).append("|");
			sb.append("SUCC_PAGEDISPLAY_CNT:").append(val.get(11)).append("|");
			sb.append("UL_DATA:").append(val.get(12)).append("|");
			sb.append("BUSS_SUCC_CNT:").append(val.get(13)).append("|");
			sb.append("BUSS_ATT_CNT:").append(val.get(14)).append("|");
			sb.append("BUSS_DELAY:").append(BUSS_DELAY).append("|");
			sb.append("HTTP_SUCC_CNT:").append(val.get(15)).append("|");
			sb.append("HTTP_FAIL_CNT:").append(val.get(16)).append("|");
			log.info("汇总后的记录："+sb.toString());
			filter_return.add(sb.toString());
			
			}
			return filter_return;
			
		}
	/**
	 * HTTP的SGW业务分组求和
	 * 
	 * @param tab
	 *            表名
	 * @param msisdns
	 *            手机号码，多个用,分割
	 * @param startTime
	 *            结束时间开始点
	 * @param endTime
	 *            结束时间结束点
	 * @return SGW业务汇总数据
	 */
	public List<String> httpSgwSvGroup(String msisdn, String startTime, String endTime) {
			// 查询详单结果
			List<String> list = getList(103,msisdn, startTime, endTime);
			log.info("list的大小："+list.size());

			if (list.size() == 0) {
				list.add(0, "0");
				log.info("list的大小为0："+list.size());
				return list;
			}
			int size = list.size();
			String DAY_ID = null;
			String HOUR_ID = null;
			String IMSI = null;
			String MSISDN= null;
			String IMEI_TAC= null;
			String TYPE_DESC= null;
			String APP_DESC= null;
			String SUBAPP_DESC= null;
			String SGW_DESC= null;
			String  tcp_conn_status=null;
			long HTTP_WAP_STATUS;
			double TCP_ATT_CNT=0;
			double TCP_SUCC_CNT=0;
			double 	TCP_SYNACK23_CNT=0;
			double 	TCP_SYNACK12_CNT=0;
			double 	TCP_RESPONSE_DELAY=0;
			double 	TCP_CONFIRM_DELAY=0;
			double 	FIRST_HTTP_RES_DELAY=0;
			double 	HTTP_RSP_FAIL_CNT=0;
			double 	BUSS_DUR=0;
			double 	HTTP_ATT_CNT=0;
			double 	DL_DATA=0;
			double 	UL_DATA=0;
			double 	SUCC_PAGEDISPLAY_CNT=0;
			double CONTENT_LENGTH=0;
			double BUSS_SUCC_CNT=0;//新加
			double BUSS_ATT_CNT=0;//新加
			double BUSS_DELAY=0;//新加
			double HTTP_SUCC_CNT=0;//新加
			double HTTP_FAIL_CNT=0;//新加
			Map<String,List<Double>> tMap = new HashMap<String,List<Double>>();
			List<String> filter_return = new ArrayList<String>();
			int i=0,j=0,k=0;
			for (String lines : list) {
				
				List<Double> filter = new ArrayList<Double>();
				String[] line = lines.split("\\|", -1);
				String keyGroupBy="-1";
				
				try {
					DAY_ID = line[0];
					HOUR_ID =line[1];
					IMSI=line[9];
					MSISDN=line[11];
					IMEI_TAC=line[12];
					TYPE_DESC=line[14];
					APP_DESC=line[49];
					SUBAPP_DESC=line[51];
					SGW_DESC=line[20];
					if(SGW_DESC==null||APP_DESC==null||SUBAPP_DESC==null)
					{
						continue;
					}
					keyGroupBy="SGW_DESC:"+SGW_DESC+"|"+"APP_DESC:"+APP_DESC+"|"+"SUBAPP_DESC:"+SUBAPP_DESC;
					//keyGroupBy=DAY_ID+"|"+HOUR_ID+"|"+IMSI+"|"+MSISDN+"|"+IMEI_TAC+"|"+TYPE_DESC+"|"+APP_DESC+"|"+SUBAPP_DESC;
					try {
					    HTTP_WAP_STATUS = Long.parseLong(addDefault(line[98], "0"));
					    CONTENT_LENGTH=Double.parseDouble(addDefault(line[111].trim(), "0"));
					} catch (NumberFormatException e1) {
					    HTTP_WAP_STATUS=-1;
					    CONTENT_LENGTH=0;
					}
					tcp_conn_status= addDefault(line[94].trim(), "0");
					BUSS_ATT_CNT=1;
					if(line[93]!=null)
					 	TCP_ATT_CNT=Double.parseDouble(line[93]);
					if (tcp_conn_status.equals("0")) {
						TCP_SUCC_CNT = TCP_ATT_CNT;
						BUSS_SUCC_CNT=1;
					}
					if(HTTP_WAP_STATUS>=300){
						HTTP_RSP_FAIL_CNT=1;
					   }
				    
					if(line[87]!=null)
					{
						i++;
						TCP_RESPONSE_DELAY=Double.parseDouble(addDefault(line[87].trim(), "0"));
					}
					if(line[88]!=null)
					{
						j++;
						TCP_CONFIRM_DELAY=Double.parseDouble(addDefault(line[88].trim(), "0"));
					}
					if(line[101]!=null)
					{
						k++;
						FIRST_HTTP_RES_DELAY=Double.parseDouble(addDefault(line[101].trim(), "0"));
					}
					UL_DATA=Double.parseDouble(addDefault(line[77].trim(), "0"));
					DL_DATA=Double.parseDouble(addDefault(line[78].trim(), "0"));
					HTTP_ATT_CNT=1;
					if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
						if((UL_DATA+DL_DATA)>CONTENT_LENGTH){
							SUCC_PAGEDISPLAY_CNT=1;  
						}
					    }
					if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
						HTTP_SUCC_CNT=1;
					    }
					else
					{
						HTTP_FAIL_CNT=1;
					}
					if(TCP_CONFIRM_DELAY>0){
					    	TCP_SYNACK12_CNT =1; 
						   }
						if(TCP_RESPONSE_DELAY>0){
						    TCP_SYNACK23_CNT =1; 
						}
					 	
					BUSS_DUR=Long.parseLong(line[60].trim())- Long.parseLong(line[59].trim()) ;
					
				} catch (Exception e) {
					e.printStackTrace();
				}
				filter.add(TCP_ATT_CNT);
				filter.add(TCP_SUCC_CNT);
				filter.add(TCP_SYNACK23_CNT);
				filter.add(TCP_SYNACK12_CNT);
				filter.add(TCP_RESPONSE_DELAY);
				filter.add(TCP_CONFIRM_DELAY);
				filter.add(FIRST_HTTP_RES_DELAY);
				filter.add(HTTP_RSP_FAIL_CNT);
				filter.add(BUSS_DUR);
				filter.add(HTTP_ATT_CNT);
				filter.add(DL_DATA);
				filter.add(SUCC_PAGEDISPLAY_CNT);
				filter.add(UL_DATA);
				filter.add(BUSS_SUCC_CNT);
				filter.add(BUSS_ATT_CNT);
				filter.add(HTTP_SUCC_CNT);
				filter.add(HTTP_FAIL_CNT);
				if(tMap.containsKey(keyGroupBy)){
					tMap.get(keyGroupBy).set(0,(tMap.get(keyGroupBy).get(0)+TCP_ATT_CNT));
					tMap.get(keyGroupBy).set(1,(tMap.get(keyGroupBy).get(1)+TCP_SUCC_CNT));
					tMap.get(keyGroupBy).set(2,(tMap.get(keyGroupBy).get(2)+TCP_SYNACK23_CNT));
					tMap.get(keyGroupBy).set(3,(tMap.get(keyGroupBy).get(3)+TCP_SYNACK12_CNT));
					tMap.get(keyGroupBy).set(4,(tMap.get(keyGroupBy).get(4)+TCP_RESPONSE_DELAY));
					tMap.get(keyGroupBy).set(5,(tMap.get(keyGroupBy).get(5)+TCP_CONFIRM_DELAY));
					tMap.get(keyGroupBy).set(6,(tMap.get(keyGroupBy).get(6)+FIRST_HTTP_RES_DELAY));
					tMap.get(keyGroupBy).set(7,(tMap.get(keyGroupBy).get(7)+HTTP_RSP_FAIL_CNT));
					tMap.get(keyGroupBy).set(8,(tMap.get(keyGroupBy).get(8)+BUSS_DUR));
					tMap.get(keyGroupBy).set(9,(tMap.get(keyGroupBy).get(9)+HTTP_ATT_CNT));
					tMap.get(keyGroupBy).set(10,(tMap.get(keyGroupBy).get(10)+DL_DATA));
					tMap.get(keyGroupBy).set(11,(tMap.get(keyGroupBy).get(11)+SUCC_PAGEDISPLAY_CNT));
					tMap.get(keyGroupBy).set(12,(tMap.get(keyGroupBy).get(12)+UL_DATA));
					tMap.get(keyGroupBy).set(13,(tMap.get(keyGroupBy).get(13)+BUSS_SUCC_CNT));
					tMap.get(keyGroupBy).set(14,(tMap.get(keyGroupBy).get(14)+BUSS_ATT_CNT));
					tMap.get(keyGroupBy).set(15,(tMap.get(keyGroupBy).get(15)+HTTP_SUCC_CNT));
					tMap.get(keyGroupBy).set(16,(tMap.get(keyGroupBy).get(16)+HTTP_FAIL_CNT));
					
				
				}else{
					tMap.put(keyGroupBy,filter);
				}

			}
			Iterator iter = tMap.entrySet().iterator();
			while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			Object key = entry.getKey();
			List<Double> val = (List<Double>)entry.getValue();
			StringBuffer sb = new StringBuffer();
			sb.append(key.toString()).append("|");
			if(i!=0)
			{
				val.set(4,val.get(4)/i);
			}
			else
			{
				val.set(4,0.0);
			}
			if(j!=0)
			{
				val.set(5,val.get(5)/j);
			}
			else
			{
				val.set(5,0.0);
			}
			if(k!=0)
			{
				val.set(6,val.get(6)/k);
			}
			else
			{
				val.set(6,0.0);
			}
			if(val.get(14)!=0)
			{	
			BUSS_DELAY=val.get(8)/val.get(14);
			}
			else
			{
			BUSS_DELAY=val.get(8);	
			}
			sb.append("TCP_ATT_CNT:").append(val.get(0)).append("|");
			sb.append("TCP_SUCC_CNT:").append(val.get(1)).append("|");
			sb.append("TCP_SYNACK23_CNT:").append(val.get(2)).append("|");
			sb.append("TCP_SYNACK12_CNT:").append(val.get(3)).append("|");
			sb.append("TCP_RESPONSE_DELAY:").append(val.get(4)).append("|");
			sb.append("TCP_CONFIRM_DELAY:").append(val.get(5)).append("|");
			sb.append("FIRST_HTTP_RES_DELAY:").append(val.get(6)).append("|");
			sb.append("HTTP_RSP_FAIL_CNT:").append(val.get(7)).append("|");
			sb.append("BUSS_DUR:").append(val.get(8)).append("|");
			sb.append("HTTP_ATT_CNT:").append(val.get(9)).append("|");
			sb.append("DL_DATA:").append(val.get(10)).append("|");
			sb.append("SUCC_PAGEDISPLAY_CNT:").append(val.get(11)).append("|");
			sb.append("UL_DATA:").append(val.get(12)).append("|");
			sb.append("BUSS_SUCC_CNT:").append(val.get(13)).append("|");
			sb.append("BUSS_ATT_CNT:").append(val.get(14)).append("|");
			sb.append("BUSS_DELAY:").append(BUSS_DELAY).append("|");
			sb.append("HTTP_SUCC_CNT:").append(val.get(15)).append("|");
			sb.append("HTTP_FAIL_CNT:").append(val.get(16)).append("|");
			log.info("汇总后的记录："+sb.toString());
			filter_return.add(sb.toString());
			
			}
			return filter_return;
			
		}
	/**
	 * HTTP的小区业务分组求和
	 * 
	 * @param tab
	 *            表名
	 * @param msisdns
	 *            手机号码，多个用,分割
	 * @param startTime
	 *            结束时间开始点
	 * @param endTime
	 *            结束时间结束点
	 * @return 小区业务汇总数据
	 */
	public List<String> httpSpSvGroup(String msisdn, String startTime, String endTime) {
			// 查询详单结果
			List<String> list = getList(103,msisdn, startTime, endTime);
			log.info("list的大小："+list.size());

			if (list.size() == 0) {
				list.add(0, "0");
				log.info("list的大小为0："+list.size());
				return list;
			}
			int size = list.size();
			String DAY_ID = null;
			String HOUR_ID = null;
			String IMSI = null;
			String MSISDN= null;
			String IMEI_TAC= null;
			String TYPE_DESC= null;
			String APP_DESC= null;
			String SUBAPP_DESC= null;
			String SERVER_IP= null;
			String ISP_DESC= null;
			String  tcp_conn_status=null;
			long HTTP_WAP_STATUS;
			double TCP_ATT_CNT=0;
			double TCP_SUCC_CNT=0;
			double 	TCP_SYNACK23_CNT=0;
			double 	TCP_SYNACK12_CNT=0;
			double 	TCP_RESPONSE_DELAY=0;
			double 	TCP_CONFIRM_DELAY=0;
			double 	FIRST_HTTP_RES_DELAY=0;
			double 	HTTP_RSP_FAIL_CNT=0;
			double 	BUSS_DUR=0;
			double 	HTTP_ATT_CNT=0;
			double 	DL_DATA=0;
			double 	UL_DATA=0;
			double 	SUCC_PAGEDISPLAY_CNT=0;
			double CONTENT_LENGTH=0;
			double BUSS_SUCC_CNT=0;//新加
			double BUSS_ATT_CNT=0;//新加
			double BUSS_DELAY=0;//新加
			double HTTP_SUCC_CNT=0;//新加
			double HTTP_FAIL_CNT=0;//新加
			Map<String,List<Double>> tMap = new HashMap<String,List<Double>>();
			List<String> filter_return = new ArrayList<String>();
			int i=0,j=0,k=0;
			for (String lines : list) {
				
				List<Double> filter = new ArrayList<Double>();
				String[] line = lines.split("\\|", -1);
				String keyGroupBy="-1";
				
				try {
					DAY_ID = line[0];
					HOUR_ID =line[1];
					IMSI=line[9];
					MSISDN=line[11];
					IMEI_TAC=line[12];
					TYPE_DESC=line[14];
					APP_DESC=line[49];
					SUBAPP_DESC=line[51];
					ISP_DESC=line[71];
					SERVER_IP=line[74];
					if(SERVER_IP==null||APP_DESC==null||SUBAPP_DESC==null)
					{
						continue;
					}
					keyGroupBy="SERVER_IP:"+SERVER_IP+"|"+"ISP_DESC:"+ISP_DESC+"|"+"APP_DESC:"+APP_DESC+"|"+"SUBAPP_DESC:"+SUBAPP_DESC;
					//keyGroupBy=DAY_ID+"|"+HOUR_ID+"|"+IMSI+"|"+MSISDN+"|"+IMEI_TAC+"|"+TYPE_DESC+"|"+APP_DESC+"|"+SUBAPP_DESC;
					try {
					    HTTP_WAP_STATUS = Long.parseLong(addDefault(line[98], "0"));
					    CONTENT_LENGTH=Double.parseDouble(addDefault(line[111].trim(), "0"));
					} catch (NumberFormatException e1) {
					    HTTP_WAP_STATUS=-1;
					    CONTENT_LENGTH=0;
					}
					tcp_conn_status= addDefault(line[94].trim(), "0");
					BUSS_ATT_CNT=1;
					if(line[93]!=null)
					 	TCP_ATT_CNT=Double.parseDouble(line[93]);
					if (tcp_conn_status.equals("0")) {
						TCP_SUCC_CNT = TCP_ATT_CNT;
						BUSS_SUCC_CNT=1;
					}
					if(HTTP_WAP_STATUS>=300){
						HTTP_RSP_FAIL_CNT=1;
					   }
				    
					if(line[87]!=null)
					{
						i++;
						TCP_RESPONSE_DELAY=Double.parseDouble(addDefault(line[87].trim(), "0"));
					}
					if(line[88]!=null)
					{
						j++;
						TCP_CONFIRM_DELAY=Double.parseDouble(addDefault(line[88].trim(), "0"));
					}
					if(line[101]!=null)
					{
						k++;
						FIRST_HTTP_RES_DELAY=Double.parseDouble(addDefault(line[101].trim(), "0"));
					}
					UL_DATA=Double.parseDouble(addDefault(line[77].trim(), "0"));
					DL_DATA=Double.parseDouble(addDefault(line[78].trim(), "0"));
					HTTP_ATT_CNT=1;
					if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
						if((UL_DATA+DL_DATA)>CONTENT_LENGTH){
							SUCC_PAGEDISPLAY_CNT=1;  
						}
					    }
					if(HTTP_WAP_STATUS>=200 && HTTP_WAP_STATUS<400){
						HTTP_SUCC_CNT=1;
					    }
					else
					{
						HTTP_FAIL_CNT=1;
					}
					if(TCP_CONFIRM_DELAY>0){
					    	TCP_SYNACK12_CNT =1; 
						   }
						if(TCP_RESPONSE_DELAY>0){
						    TCP_SYNACK23_CNT =1; 
						}
					 	
					BUSS_DUR=Long.parseLong(line[60].trim())- Long.parseLong(line[59].trim()) ;
					
				} catch (Exception e) {
					e.printStackTrace();
				}
				filter.add(TCP_ATT_CNT);
				filter.add(TCP_SUCC_CNT);
				filter.add(TCP_SYNACK23_CNT);
				filter.add(TCP_SYNACK12_CNT);
				filter.add(TCP_RESPONSE_DELAY);
				filter.add(TCP_CONFIRM_DELAY);
				filter.add(FIRST_HTTP_RES_DELAY);
				filter.add(HTTP_RSP_FAIL_CNT);
				filter.add(BUSS_DUR);
				filter.add(HTTP_ATT_CNT);
				filter.add(DL_DATA);
				filter.add(SUCC_PAGEDISPLAY_CNT);
				filter.add(UL_DATA);
				filter.add(BUSS_SUCC_CNT);
				filter.add(BUSS_ATT_CNT);
				filter.add(HTTP_SUCC_CNT);
				filter.add(HTTP_FAIL_CNT);
				if(tMap.containsKey(keyGroupBy)){
					tMap.get(keyGroupBy).set(0,(tMap.get(keyGroupBy).get(0)+TCP_ATT_CNT));
					tMap.get(keyGroupBy).set(1,(tMap.get(keyGroupBy).get(1)+TCP_SUCC_CNT));
					tMap.get(keyGroupBy).set(2,(tMap.get(keyGroupBy).get(2)+TCP_SYNACK23_CNT));
					tMap.get(keyGroupBy).set(3,(tMap.get(keyGroupBy).get(3)+TCP_SYNACK12_CNT));
					tMap.get(keyGroupBy).set(4,(tMap.get(keyGroupBy).get(4)+TCP_RESPONSE_DELAY));
					tMap.get(keyGroupBy).set(5,(tMap.get(keyGroupBy).get(5)+TCP_CONFIRM_DELAY));
					tMap.get(keyGroupBy).set(6,(tMap.get(keyGroupBy).get(6)+FIRST_HTTP_RES_DELAY));
					tMap.get(keyGroupBy).set(7,(tMap.get(keyGroupBy).get(7)+HTTP_RSP_FAIL_CNT));
					tMap.get(keyGroupBy).set(8,(tMap.get(keyGroupBy).get(8)+BUSS_DUR));
					tMap.get(keyGroupBy).set(9,(tMap.get(keyGroupBy).get(9)+HTTP_ATT_CNT));
					tMap.get(keyGroupBy).set(10,(tMap.get(keyGroupBy).get(10)+DL_DATA));
					tMap.get(keyGroupBy).set(11,(tMap.get(keyGroupBy).get(11)+SUCC_PAGEDISPLAY_CNT));
					tMap.get(keyGroupBy).set(12,(tMap.get(keyGroupBy).get(12)+UL_DATA));
					tMap.get(keyGroupBy).set(13,(tMap.get(keyGroupBy).get(13)+BUSS_SUCC_CNT));
					tMap.get(keyGroupBy).set(14,(tMap.get(keyGroupBy).get(14)+BUSS_ATT_CNT));
					tMap.get(keyGroupBy).set(15,(tMap.get(keyGroupBy).get(15)+HTTP_SUCC_CNT));
					tMap.get(keyGroupBy).set(16,(tMap.get(keyGroupBy).get(16)+HTTP_FAIL_CNT));
					
				
				}else{
					tMap.put(keyGroupBy,filter);
				}

			}
			Iterator iter = tMap.entrySet().iterator();
			while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			Object key = entry.getKey();
			List<Double> val = (List<Double>)entry.getValue();
			StringBuffer sb = new StringBuffer();
			sb.append(key.toString()).append("|");
			if(i!=0)
			{
				val.set(4,val.get(4)/i);
			}
			else
			{
				val.set(4,0.0);
			}
			if(j!=0)
			{
				val.set(5,val.get(5)/j);
			}
			else
			{
				val.set(5,0.0);
			}
			if(k!=0)
			{
				val.set(6,val.get(6)/k);
			}
			else
			{
				val.set(6,0.0);
			}
			if(val.get(14)!=0)
			{	
			BUSS_DELAY=val.get(8)/val.get(14);
			}
			else
			{
			BUSS_DELAY=val.get(8);	
			}
			sb.append("TCP_ATT_CNT:").append(val.get(0)).append("|");
			sb.append("TCP_SUCC_CNT:").append(val.get(1)).append("|");
			sb.append("TCP_SYNACK23_CNT:").append(val.get(2)).append("|");
			sb.append("TCP_SYNACK12_CNT:").append(val.get(3)).append("|");
			sb.append("TCP_RESPONSE_DELAY:").append(val.get(4)).append("|");
			sb.append("TCP_CONFIRM_DELAY:").append(val.get(5)).append("|");
			sb.append("FIRST_HTTP_RES_DELAY:").append(val.get(6)).append("|");
			sb.append("HTTP_RSP_FAIL_CNT:").append(val.get(7)).append("|");
			sb.append("BUSS_DUR:").append(val.get(8)).append("|");
			sb.append("HTTP_ATT_CNT:").append(val.get(9)).append("|");
			sb.append("DL_DATA:").append(val.get(10)).append("|");
			sb.append("SUCC_PAGEDISPLAY_CNT:").append(val.get(11)).append("|");
			sb.append("UL_DATA:").append(val.get(12)).append("|");
			sb.append("BUSS_SUCC_CNT:").append(val.get(13)).append("|");
			sb.append("BUSS_ATT_CNT:").append(val.get(14)).append("|");
			sb.append("BUSS_DELAY:").append(BUSS_DELAY).append("|");
			sb.append("HTTP_SUCC_CNT:").append(val.get(15)).append("|");
			sb.append("HTTP_FAIL_CNT:").append(val.get(16)).append("|");
			log.info("汇总后的记录："+sb.toString());
			filter_return.add(sb.toString());
			
			}
			return filter_return;
			
		}
	/**
	 * DNS的用户粒度指标分组求和
	 * 
	 * @param tab
	 *            表名
	 * @param msisdns
	 *            手机号码，多个用,分割
	 * @param startTime
	 *            结束时间开始点
	 * @param endTime
	 *            结束时间结束点
	 * @return 用户粒度指标汇总数据
	 */
	public List<String> DnsUserGroup(String msisdn, String startTime, String endTime) {
		// 查询详单结果
		List<String> list = getList(101,msisdn, startTime, endTime);
		log.info("list的大小："+list.size());

		if (list.size() == 0) {
			list.add(0, "0");
			log.info("list的大小为0："+list.size());
			return list;
		}
		int size = list.size();
		String DAY_ID = null;
		String HOUR_ID = null;
		String IMSI = null;
		String MSISDN= null;
		long DNS_RESP_CODE= 0;
		double DNS_RESP_FAIL_CNT=0;
        double DNS_RESP_CNT=0;
        double DNS_RESP_DELAY=0;
        double DNS_RESP_SUCC_CNT=0;
		Map<String,List<Double>> tMap = new HashMap<String,List<Double>>();
		List<String> filter_return = new ArrayList<String>();
		for (String lines : list) {
			
			List<Double> filter = new ArrayList<Double>();
			String[] line = lines.split("\\|", -1);
			String keyGroupBy="-1";
			//log.info(lines);
			//log.info("DNS_RESP_CODE:"+line[93]);
			try {
				DAY_ID = line[0];
				HOUR_ID =line[1];
				IMSI=line[9];
				MSISDN=line[11];
				if(DAY_ID==null||HOUR_ID==null||IMSI==null||MSISDN==null)
				{
					continue;
				}
				keyGroupBy="DAY_ID:"+DAY_ID+"|"+"HOUR_ID:"+HOUR_ID+"|"+"IMSI:"+IMSI+"|"+"MSISDN:"+MSISDN;
				try {
					DNS_RESP_CODE = Long.parseLong(addDefault(line[93], "0"));
				} catch (NumberFormatException e1) {
					DNS_RESP_CODE=-1;
				}
				if (0 < DNS_RESP_CODE && DNS_RESP_CODE <= 5) {
					DNS_RESP_FAIL_CNT = 1;// 失败次数
				}
				if (DNS_RESP_CODE <= 5) { // 请求次数
					DNS_RESP_CNT = 1;
				}		
				if (DNS_RESP_CODE == 0) { // 成功次数
					DNS_RESP_SUCC_CNT = 1;
				}
				DNS_RESP_DELAY=Long.parseLong(line[50].trim())- Long.parseLong(line[49].trim()) ;
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			filter.add(DNS_RESP_FAIL_CNT);
			filter.add(DNS_RESP_CNT);
			filter.add(DNS_RESP_DELAY);
			filter.add(DNS_RESP_SUCC_CNT);
			if(tMap.containsKey(keyGroupBy)){
				tMap.get(keyGroupBy).set(0,(tMap.get(keyGroupBy).get(0)+DNS_RESP_FAIL_CNT));
				tMap.get(keyGroupBy).set(1,(tMap.get(keyGroupBy).get(1)+DNS_RESP_CNT));
				tMap.get(keyGroupBy).set(2,(tMap.get(keyGroupBy).get(2)+DNS_RESP_DELAY));
				tMap.get(keyGroupBy).set(3,(tMap.get(keyGroupBy).get(3)+DNS_RESP_SUCC_CNT));
			}
			else{
				tMap.put(keyGroupBy,filter);
			}

		}
		Iterator iter = tMap.entrySet().iterator();
		while (iter.hasNext()) {
		Map.Entry entry = (Map.Entry) iter.next();
		Object key = entry.getKey();
		List<Double> val = (List<Double>)entry.getValue();
		StringBuffer sb = new StringBuffer();
		sb.append(key.toString()).append("|");

			sb.append("DNS_RESP_FAIL_CNT:").append(val.get(0)).append("|");
			sb.append("DNS_RESP_CNT:").append(val.get(1)).append("|");
			sb.append("DNS_RESP_DELAY:").append(val.get(2)).append("|");
			sb.append("DNS_RESP_SUCC_CNT:").append(val.get(3)).append("|");
		log.info("汇总后的记录："+sb.toString());
		filter_return.add(sb.toString());
		
		}
		return filter_return;
	}
	/**
	 * DNS的用户粒度原因码指标分组求和
	 * 
	 * @param tab
	 *            表名
	 * @param msisdns
	 *            手机号码，多个用,分割
	 * @param startTime
	 *            结束时间开始点
	 * @param endTime
	 *            结束时间结束点
	 * @return 用户粒度原因码指标汇总数据
	 */
	public List<String> DnsUserFcauseGroup(String msisdn, String startTime, String endTime,String cause) {
		// 查询详单结果
		List<String> list = getList(101,msisdn, startTime, endTime);
		log.info("list的大小："+list.size());

		if (list.size() == 0) {
			list.add(0, "0");
			log.info("list的大小为0："+list.size());
			return list;
		}
		int size = list.size();
		String DAY_ID = null;
		String HOUR_ID = null;
		String IMSI = null;
		String MSISDN= null;
		long DNS_RESP_CODE= 0;
		double DNS_RESP_FAIL_CNT=0;
        double DNS_RESP_CNT=0;
        double DNS_RESP_DELAY=0;
        double DNS_RESP_SUCC_CNT=0;
        String[] cause_list = cause.split("\\,", -1);
		Map<String,List<Double>> tMap = new HashMap<String,List<Double>>();
		List<String> filter_return = new ArrayList<String>();
		for (String lines : list) {
			boolean flag=true;
			List<Double> filter = new ArrayList<Double>();
			String[] line = lines.split("\\|", -1);
			String keyGroupBy="-1";
			
			try {
				DAY_ID = line[0];
				HOUR_ID =line[1];
				IMSI=line[9];
				MSISDN=line[11];
				if(DAY_ID==null||HOUR_ID==null||IMSI==null||MSISDN==null)
				{
					continue;
				}
				keyGroupBy="DAY_ID:"+DAY_ID+"|"+"HOUR_ID:"+HOUR_ID+"|"+"IMSI:"+IMSI+"|"+"MSISDN:"+MSISDN;
				
				try {
					DNS_RESP_CODE = Long.parseLong(addDefault(line[93], "0"));
				} catch (NumberFormatException e1) {
					DNS_RESP_CODE=-1;
				}
				//是否在原因码里面
				for (String cause_temp : cause_list)
				{
				
				if(String.valueOf(DNS_RESP_CODE).equals(cause_temp))
				{
					flag=false;
					log.info("存在相同原因码不能跳过，原因码DNS_RESP_CODE："+DNS_RESP_CODE);
					break;
				}
				}
				if(flag)
				{
					continue;
				}
				if (0 < DNS_RESP_CODE && DNS_RESP_CODE <= 5) {
					DNS_RESP_FAIL_CNT = 1;
				}
				if (DNS_RESP_CODE <= 5) { // 请求次数
					DNS_RESP_CNT = 1;
				}				 	
				if (DNS_RESP_CODE == 0) { // 成功次数
					DNS_RESP_SUCC_CNT = 1;
				}
				DNS_RESP_DELAY=Long.parseLong(line[50].trim())- Long.parseLong(line[49].trim()) ;
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			filter.add(DNS_RESP_FAIL_CNT);
			filter.add(DNS_RESP_CNT);
			filter.add(DNS_RESP_DELAY);
			filter.add(DNS_RESP_SUCC_CNT);
			if(tMap.containsKey(keyGroupBy)){
				tMap.get(keyGroupBy).set(0,(tMap.get(keyGroupBy).get(0)+DNS_RESP_FAIL_CNT));
				tMap.get(keyGroupBy).set(1,(tMap.get(keyGroupBy).get(1)+DNS_RESP_CNT));
				tMap.get(keyGroupBy).set(2,(tMap.get(keyGroupBy).get(2)+DNS_RESP_DELAY));
				tMap.get(keyGroupBy).set(3,(tMap.get(keyGroupBy).get(3)+DNS_RESP_SUCC_CNT));
			}
			else{
				tMap.put(keyGroupBy,filter);
			}

		}
		Iterator iter = tMap.entrySet().iterator();
		while (iter.hasNext()) {
		Map.Entry entry = (Map.Entry) iter.next();
		Object key = entry.getKey();
		List<Double> val = (List<Double>)entry.getValue();
		StringBuffer sb = new StringBuffer();
		sb.append(key.toString()).append("|");

			sb.append("DNS_RESP_FAIL_CNT:").append(val.get(0)).append("|");
			sb.append("DNS_RESP_CNT:").append(val.get(1)).append("|");
			sb.append("DNS_RESP_DELAY:").append(val.get(2)).append("|");
			sb.append("DNS_RESP_SUCC_CNT:").append(val.get(3)).append("|");
		log.info("汇总后的记录："+sb.toString());
		filter_return.add(sb.toString());
		
		}
		
		return filter_return;
	}
	/**
	 * MME的用户粒度指标分组求和
	 * 
	 * @param tab
	 *            表名
	 * @param msisdns
	 *            手机号码，多个用,分割
	 * @param startTime
	 *            结束时间开始点
	 * @param endTime
	 *            结束时间结束点
	 * @return 用户粒度指标汇总数据
	 */
	public List<String> MmeUserGroup(String msisdn, String startTime, String endTime) {
		// 查询详单结果
		List<String> list = getList(110,msisdn, startTime, endTime);
		log.info("list的大小："+list.size());

		if (list.size() == 0) {
			list.add(0, "0");
			log.info("list的大小为0："+list.size());
			return list;
		}
		int size = list.size();
		String DAY_ID = null;
		String HOUR_ID = null;
		String IMSI = null;
		String MSISDN= null;
		long PROCEDURE_TYPE_L= 9999;
		long KEYWORD2= 0;
		long PROCEDURE_STATUS_L= 9999;
		//addDefault(xdrdata[KEYWORD1], "0")
		double MOBILITY_TAU_FAIL_CNT=0;
        double MOBILITY_TAU_REQ_CNT=0;
        double DEFAULT_BEA_SETUP_REQS_CNT=0;
        double DEFAULT_BEA_SETUP_FAIL_CNT=0;
        double ATTACH_FAIL_CNT=0;
        double ATTACH_REQ_CNT=0;
        double DEFAULT_BEA_SETUP_SUC_CNT=0;
        double DEFAULT_BEA_SETUP_TIMEOUT_CNT=0;
        double MOBILITY_TAU_SUC_CNT=0;
        double ATTACH_SUC_CNT=0;
        double TAU_REQ_CNT=0;
        double TAU_SUC_CNT=0;
        double TAU_FAIL_CNT=0;
		Map<String,List<Double>> tMap = new HashMap<String,List<Double>>();
		List<String> filter_return = new ArrayList<String>();
		for (String lines : list) {
			
			List<Double> filter = new ArrayList<Double>();
			String[] line = lines.split("\\|", -1);
			String keyGroupBy="-1";
		//	log.info(lines);
		//	log.info("PROCEDURE_TYPE_L:"+line[16]+"KEYWORD2:"+line[25]+"PROCEDURE_STATUS_L:"+line[19]);
			try {
				DAY_ID = line[0];
				HOUR_ID =line[1];
				IMSI=line[9];
				MSISDN=line[11];
				if(DAY_ID==null||HOUR_ID==null||IMSI==null||MSISDN==null)
				{
					continue;
				}
				keyGroupBy="DAY_ID:"+DAY_ID+"|"+"HOUR_ID:"+HOUR_ID+"|"+"IMSI:"+IMSI+"|"+"MSISDN:"+MSISDN;
				try {
					PROCEDURE_TYPE_L = Long.parseLong(addDefault(line[16], "9999"));
					KEYWORD2 = Long.parseLong(addDefault(line[25], "0"));
					PROCEDURE_STATUS_L = Long.parseLong(addDefault(line[19], "9999"));
				} catch (NumberFormatException e1) {
					PROCEDURE_TYPE_L=9999;
					KEYWORD2=0;
					PROCEDURE_STATUS_L=9999;
				}
				if (PROCEDURE_TYPE_L == 5 && KEYWORD2 == 0) {
					MOBILITY_TAU_REQ_CNT = 1; // 移动性tau请求次数
					if (PROCEDURE_STATUS_L == 0) {
						MOBILITY_TAU_SUC_CNT = 1; // 移动性tau成功次数
					} else if (PROCEDURE_STATUS_L == 1) {
						MOBILITY_TAU_FAIL_CNT = 1; // 移动性tau失败次数
					}
				}
				if (PROCEDURE_TYPE_L == 1) {
					ATTACH_REQ_CNT = 1; // 附着请求总次数
					if (PROCEDURE_STATUS_L == 0) {
						ATTACH_SUC_CNT = 1; // 附着成功次数
					} else if (PROCEDURE_STATUS_L != 0) {
						ATTACH_FAIL_CNT = 1; // 附着失败次数
					}
				}
				if (PROCEDURE_TYPE_L == 5 && PROCEDURE_STATUS_L != 255) {
					TAU_REQ_CNT = 1; // 跟踪区更新请求总次数
					if (PROCEDURE_STATUS_L == 0) {
					//	tau_res_delay += tmp_total_delay; // 跟踪区更新响应总时延
						TAU_SUC_CNT = 1; // 跟踪区更新成功次数
					} else if (PROCEDURE_STATUS_L == 1) {
						TAU_FAIL_CNT = 1; // 跟踪区更新失败次数
					}
				}
				if (PROCEDURE_TYPE_L == 7) {
					DEFAULT_BEA_SETUP_REQS_CNT = 1; // 默认eps承载建立请求次数
					if (PROCEDURE_STATUS_L == 0) {
						DEFAULT_BEA_SETUP_SUC_CNT = 1; // 默认eps承载建立成功次数
					} else if (PROCEDURE_STATUS_L != 0) {
						DEFAULT_BEA_SETUP_FAIL_CNT = 1; // 默认eps承载建立失败次数
						if (PROCEDURE_STATUS_L == 255) {
							DEFAULT_BEA_SETUP_TIMEOUT_CNT = 1; // 默认eps承载建立超时次数
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			filter.add(MOBILITY_TAU_REQ_CNT);
			filter.add(MOBILITY_TAU_FAIL_CNT);
			filter.add(ATTACH_REQ_CNT);
			filter.add(ATTACH_FAIL_CNT);
			filter.add(DEFAULT_BEA_SETUP_REQS_CNT);
			filter.add(DEFAULT_BEA_SETUP_FAIL_CNT);
			filter.add(DEFAULT_BEA_SETUP_SUC_CNT);
			filter.add(DEFAULT_BEA_SETUP_TIMEOUT_CNT);
			filter.add(MOBILITY_TAU_SUC_CNT);
			filter.add(ATTACH_SUC_CNT);
			filter.add(TAU_REQ_CNT);
			filter.add(TAU_SUC_CNT);
			filter.add(TAU_FAIL_CNT);
			if(tMap.containsKey(keyGroupBy)){
				tMap.get(keyGroupBy).set(0,(tMap.get(keyGroupBy).get(0)+MOBILITY_TAU_REQ_CNT));
				tMap.get(keyGroupBy).set(1,(tMap.get(keyGroupBy).get(1)+MOBILITY_TAU_FAIL_CNT));
				tMap.get(keyGroupBy).set(2,(tMap.get(keyGroupBy).get(2)+ATTACH_REQ_CNT));
				tMap.get(keyGroupBy).set(3,(tMap.get(keyGroupBy).get(3)+ATTACH_FAIL_CNT));
				tMap.get(keyGroupBy).set(4,(tMap.get(keyGroupBy).get(4)+DEFAULT_BEA_SETUP_REQS_CNT));
				tMap.get(keyGroupBy).set(5,(tMap.get(keyGroupBy).get(5)+DEFAULT_BEA_SETUP_FAIL_CNT));
				tMap.get(keyGroupBy).set(6,(tMap.get(keyGroupBy).get(6)+DEFAULT_BEA_SETUP_SUC_CNT));
				tMap.get(keyGroupBy).set(7,(tMap.get(keyGroupBy).get(7)+DEFAULT_BEA_SETUP_TIMEOUT_CNT));
				tMap.get(keyGroupBy).set(8,(tMap.get(keyGroupBy).get(8)+MOBILITY_TAU_SUC_CNT));
				tMap.get(keyGroupBy).set(9,(tMap.get(keyGroupBy).get(9)+ATTACH_SUC_CNT));
				tMap.get(keyGroupBy).set(10,(tMap.get(keyGroupBy).get(9)+TAU_REQ_CNT));
				tMap.get(keyGroupBy).set(11,(tMap.get(keyGroupBy).get(9)+TAU_SUC_CNT));
				tMap.get(keyGroupBy).set(12,(tMap.get(keyGroupBy).get(9)+TAU_FAIL_CNT));
				
			}
			else{
				tMap.put(keyGroupBy,filter);
			}

		}
		Iterator iter = tMap.entrySet().iterator();
		while (iter.hasNext()) {
		Map.Entry entry = (Map.Entry) iter.next();
		Object key = entry.getKey();
		List<Double> val = (List<Double>)entry.getValue();
		StringBuffer sb = new StringBuffer();
		sb.append(key.toString()).append("|");

		sb.append("MOBILITY_TAU_REQ_CNT:").append(val.get(0)).append("|");	
		sb.append("MOBILITY_TAU_FAIL_CNT:").append(val.get(1)).append("|");	
		sb.append("ATTACH_REQ_CNT:").append(val.get(2)).append("|");	
		sb.append("ATTACH_FAIL_CNT:").append(val.get(3)).append("|");	
		sb.append("DEFAULT_BEA_SETUP_REQS_CNT:").append(val.get(4)).append("|");	
		sb.append("DEFAULT_BEA_SETUP_FAIL_CNT:").append(val.get(5)).append("|");	
		sb.append("DEFAULT_BEA_SETUP_SUC_CNT:").append(val.get(6)).append("|");	
		sb.append("DEFAULT_BEA_SETUP_TIMEOUT_CNT:").append(val.get(7)).append("|");	
		sb.append("MOBILITY_TAU_SUC_CNT:").append(val.get(8)).append("|");	
		sb.append("ATTACH_SUC_CNT:").append(val.get(9)).append("|");	
		sb.append("TAU_REQ_CNT:").append(val.get(10)).append("|");	
		sb.append("TAU_SUC_CNT:").append(val.get(11)).append("|");	
		sb.append("TAU_FAIL_CNT:").append(val.get(12)).append("|");	

		log.info("汇总后的记录："+sb.toString());
		filter_return.add(sb.toString());
		}
		return filter_return;
	}
	/**
	 * MME的用户粒度原因码指标分组求和
	 * 
	 * @param tab
	 *            表名
	 * @param msisdns
	 *            手机号码，多个用,分割
	 * @param startTime
	 *            结束时间开始点
	 * @param endTime
	 *            结束时间结束点
	 * @return 用户粒度指原因码标汇总数据
	 */
	public List<String> MmeUserFcauseGroup(String msisdn, String startTime, String endTime, String cause) {
		// 查询详单结果
		List<String> list = getList(110,msisdn, startTime, endTime);
		log.info("list的大小："+list.size());

		if (list.size() == 0) {
			list.add(0, "0");
			log.info("list的大小为0："+list.size());
			return list;
		}
		int size = list.size();
		String DAY_ID = null;
		String HOUR_ID = null;
		String IMSI = null;
		String MSISDN= null;
		long PROCEDURE_TYPE_L= 9999;
		long KEYWORD2= 0;
		long PROCEDURE_STATUS_L= 9999;
		long FAILURE_CAUSE=9999;
		//addDefault(xdrdata[KEYWORD1], "0")
		double MOBILITY_TAU_FAIL_CNT=0;
        double MOBILITY_TAU_REQ_CNT=0;
        double DEFAULT_BEA_SETUP_REQS_CNT=0;
        double DEFAULT_BEA_SETUP_FAIL_CNT=0;
        double ATTACH_FAIL_CNT=0;
        double ATTACH_REQ_CNT=0;
        double DEFAULT_BEA_SETUP_SUC_CNT=0;
        double DEFAULT_BEA_SETUP_TIMEOUT_CNT=0;
        double MOBILITY_TAU_SUC_CNT=0;
        double ATTACH_SUC_CNT=0;
        double TAU_REQ_CNT=0;
        double TAU_SUC_CNT=0;
        double TAU_FAIL_CNT=0;
        String[] cause_list = cause.split("\\,", -1);
		Map<String,List<Double>> tMap = new HashMap<String,List<Double>>();
		List<String> filter_return = new ArrayList<String>();
		for (String lines : list) {
			boolean flag=true;
			List<Double> filter = new ArrayList<Double>();
			String[] line = lines.split("\\|", -1);
			String keyGroupBy="-1";
			
			try {
				DAY_ID = line[0];
				HOUR_ID =line[1];
				IMSI=line[9];
				MSISDN=line[11];
				if(DAY_ID==null||HOUR_ID==null||IMSI==null||MSISDN==null)
				{
					continue;
				}
				keyGroupBy="DAY_ID:"+DAY_ID+"|"+"HOUR_ID:"+HOUR_ID+"|"+"IMSI:"+IMSI+"|"+"MSISDN:"+MSISDN;
				try {
					PROCEDURE_TYPE_L = Long.parseLong(addDefault(line[16], "9999"));
					KEYWORD2 = Long.parseLong(addDefault(line[25], "0"));
					PROCEDURE_STATUS_L = Long.parseLong(addDefault(line[19], "9999"));
					FAILURE_CAUSE=Long.parseLong(addDefault(line[21], "9999"));
				} catch (NumberFormatException e1) {
					PROCEDURE_TYPE_L=9999;
					KEYWORD2=0;
					PROCEDURE_STATUS_L=9999;
					FAILURE_CAUSE=9999;
				}
				//是否在原因码里面
				for (String cause_temp : cause_list)
				{
				
				if(String.valueOf(FAILURE_CAUSE).equals(cause_temp))
				{
					flag=false;
					log.info("存在相同原因码不能跳过，原因码FAILURE_CAUSE："+FAILURE_CAUSE);
					break;
				}
				}
				if(flag)
				{
					continue;
				}
				if (PROCEDURE_TYPE_L == 5 && KEYWORD2 == 0) {
					MOBILITY_TAU_REQ_CNT = 1; // 移动性tau请求次数
					if (PROCEDURE_STATUS_L == 0) {
						MOBILITY_TAU_SUC_CNT = 1; // 移动性tau成功次数
					} else if (PROCEDURE_STATUS_L == 1) {
						MOBILITY_TAU_FAIL_CNT = 1; // 移动性tau失败次数
					}
				}
				if (PROCEDURE_TYPE_L == 5 && PROCEDURE_STATUS_L != 255) {
					TAU_REQ_CNT = 1; // 跟踪区更新请求总次数
					if (PROCEDURE_STATUS_L == 0) {
					//	tau_res_delay += tmp_total_delay; // 跟踪区更新响应总时延
						TAU_SUC_CNT = 1; // 跟踪区更新成功次数
					} else if (PROCEDURE_STATUS_L == 1) {
						TAU_FAIL_CNT = 1; // 跟踪区更新失败次数
					}
				}
				if (PROCEDURE_TYPE_L == 1) {
					ATTACH_REQ_CNT = 1; // 附着请求总次数
					if (PROCEDURE_STATUS_L == 0) {
						ATTACH_SUC_CNT += 1; // 附着成功次数
					} else if (PROCEDURE_STATUS_L != 0) {
						ATTACH_FAIL_CNT += 1; // 附着失败次数
					}
				}
				if (PROCEDURE_TYPE_L == 7) {
					DEFAULT_BEA_SETUP_REQS_CNT = 1; // 默认eps承载建立请求次数
					if (PROCEDURE_STATUS_L == 0) {
						DEFAULT_BEA_SETUP_SUC_CNT = 1; // 默认eps承载建立成功次数
					} else if (PROCEDURE_STATUS_L != 0) {
						DEFAULT_BEA_SETUP_FAIL_CNT = 1; // 默认eps承载建立失败次数
						if (PROCEDURE_STATUS_L == 255) {
							DEFAULT_BEA_SETUP_TIMEOUT_CNT = 1; // 默认eps承载建立超时次数
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			filter.add(MOBILITY_TAU_REQ_CNT);
			filter.add(MOBILITY_TAU_FAIL_CNT);
			filter.add(ATTACH_REQ_CNT);
			filter.add(ATTACH_FAIL_CNT);
			filter.add(DEFAULT_BEA_SETUP_REQS_CNT);
			filter.add(DEFAULT_BEA_SETUP_FAIL_CNT);
			filter.add(DEFAULT_BEA_SETUP_SUC_CNT);
			filter.add(DEFAULT_BEA_SETUP_TIMEOUT_CNT);
			filter.add(MOBILITY_TAU_SUC_CNT);
			filter.add(ATTACH_SUC_CNT);
			filter.add(TAU_REQ_CNT);
			filter.add(TAU_SUC_CNT);
			filter.add(TAU_FAIL_CNT);
			if(tMap.containsKey(keyGroupBy)){
				tMap.get(keyGroupBy).set(0,(tMap.get(keyGroupBy).get(0)+MOBILITY_TAU_REQ_CNT));
				tMap.get(keyGroupBy).set(1,(tMap.get(keyGroupBy).get(1)+MOBILITY_TAU_FAIL_CNT));
				tMap.get(keyGroupBy).set(2,(tMap.get(keyGroupBy).get(2)+ATTACH_REQ_CNT));
				tMap.get(keyGroupBy).set(3,(tMap.get(keyGroupBy).get(3)+ATTACH_FAIL_CNT));
				tMap.get(keyGroupBy).set(4,(tMap.get(keyGroupBy).get(4)+DEFAULT_BEA_SETUP_REQS_CNT));
				tMap.get(keyGroupBy).set(5,(tMap.get(keyGroupBy).get(5)+DEFAULT_BEA_SETUP_FAIL_CNT));
				tMap.get(keyGroupBy).set(6,(tMap.get(keyGroupBy).get(6)+DEFAULT_BEA_SETUP_SUC_CNT));
				tMap.get(keyGroupBy).set(7,(tMap.get(keyGroupBy).get(7)+DEFAULT_BEA_SETUP_TIMEOUT_CNT));
				tMap.get(keyGroupBy).set(8,(tMap.get(keyGroupBy).get(8)+MOBILITY_TAU_SUC_CNT));
				tMap.get(keyGroupBy).set(9,(tMap.get(keyGroupBy).get(9)+ATTACH_SUC_CNT));
				tMap.get(keyGroupBy).set(10,(tMap.get(keyGroupBy).get(9)+TAU_REQ_CNT));
				tMap.get(keyGroupBy).set(11,(tMap.get(keyGroupBy).get(9)+TAU_SUC_CNT));
				tMap.get(keyGroupBy).set(12,(tMap.get(keyGroupBy).get(9)+TAU_FAIL_CNT));
				
			}
			else{
				tMap.put(keyGroupBy,filter);
			}

		}
		Iterator iter = tMap.entrySet().iterator();
		while (iter.hasNext()) {
		Map.Entry entry = (Map.Entry) iter.next();
		Object key = entry.getKey();
		List<Double> val = (List<Double>)entry.getValue();
		StringBuffer sb = new StringBuffer();
		sb.append(key.toString()).append("|");

		sb.append("MOBILITY_TAU_REQ_CNT:").append(val.get(0)).append("|");	
		sb.append("MOBILITY_TAU_FAIL_CNT:").append(val.get(1)).append("|");	
		sb.append("ATTACH_REQ_CNT:").append(val.get(2)).append("|");	
		sb.append("ATTACH_FAIL_CNT:").append(val.get(3)).append("|");	
		sb.append("DEFAULT_BEA_SETUP_REQS_CNT:").append(val.get(4)).append("|");	
		sb.append("DEFAULT_BEA_SETUP_FAIL_CNT:").append(val.get(5)).append("|");	
		sb.append("DEFAULT_BEA_SETUP_SUC_CNT:").append(val.get(6)).append("|");	
		sb.append("DEFAULT_BEA_SETUP_TIMEOUT_CNT:").append(val.get(7)).append("|");	
		sb.append("MOBILITY_TAU_SUC_CNT:").append(val.get(8)).append("|");	
		sb.append("ATTACH_SUC_CNT:").append(val.get(9)).append("|");	
		sb.append("TAU_REQ_CNT:").append(val.get(10)).append("|");	
		sb.append("TAU_SUC_CNT:").append(val.get(11)).append("|");	
		sb.append("TAU_FAIL_CNT:").append(val.get(12)).append("|");	

		log.info("汇总后的记录："+sb.toString());
		filter_return.add(sb.toString());
		}

		return filter_return;
	}
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
				startTime_1 = DateUtils.paserTime(line[59], "yyyy-MM-dd HH:mm:ss");
				endTime_1 = DateUtils.paserTime(line[60], "yyyy-MM-dd HH:mm:ss");
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
			sb_23g.append(line_23g[41]).append("|");// url
			sb_23g.append(line_23g[22]).append("-").append(line_23g[24]).append("|");// app-subapp
			sb_23g.append(line_23g[42]).append("|");// 上行流量
			sb_23g.append(line_23g[43]).append("|");// 下行流量
			sb_23g.append(Double.parseDouble(line_23g[42]) + Double.parseDouble(line_23g[43])).append("|");// 总流量
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
		List<String> order = sortIndex(list, 60);
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
			String endTime, String cellName) {
		// 获取小时内的查询结果
		List<String> list = getList(tab, msisdns, startTime, endTime);
		// 通过时间和小区名称对list进行过滤
		List<String> filter = filterList(list, cellName, startTime, endTime);
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
			String startTime, String endTime, String cellName, int pageSize,
			int index) {
		// 获取小时内的查询结果
		List<String> list = getList(tab, msisdns, startTime, endTime);
		// 判断是否查询到结果
		if (list.size() == 0) {
			return new ArrayList<String>();
		}
		// 通过时间和小区名称对list进行过滤
		List<String> filter = filterList(list, cellName, startTime, endTime);
		// 对list进行排序保证分页显示
		// Collections.sort(filter);
		// 按照时间进行排序，轨迹详单位置在18位
		List<String> order = sortIndex(filter, 18);
		// 分页
		List<String> result = pageList(order, pageSize, index);
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

		List<String> list = getList(tab, msisdns, startTime, endTime);

		// 判断是否查询到结果
		if (list.size() == 0) {
			return new ArrayList<String>();
		}
		// 按照时间进行排序
		List<String> orderList = sortIndex(list, 18);
		// 进行合并并截取合适的字段
		List<String> result = new ArrayList<String>();
		// 获取第一行数据作为比较
		String[] s0 = pattern.split(orderList.get(0));
		String tmpCell = s0[7];
		String tmpStart = s0[18];
		String tmpEnd = s0[18];
		String tmpJd = s0[15];
		String tmpWd = s0[16];
		String proType = getProType(s0[19]);
		for (int z = 1; z < orderList.size(); z++) {
			String[] s = pattern.split(orderList.get(z));
			String time = s[18];
			String cell = s[7];
			String jd = s[15];
			String wd = s[16];
			String type = getProType(s[19]);
			if (cell.equals(tmpCell)) {
				tmpEnd = time;
				if (StringUtils.isNotBlank(type) && !proType.contains(type)) {
					proType += proType + type;
				}
			} else {
				result.add(tmpCell + "|" + tmpStart + "|" + tmpEnd + "|"
						+ tmpJd + "|" + tmpWd + "|" + proType);
				tmpCell = cell;
				tmpStart = time;
				tmpEnd = time;
				tmpJd = jd;
				tmpWd = wd;
				proType = type;
			}
		}
		// 增加最后一条记录
		result.add(tmpCell + "|" + tmpStart + "|" + tmpEnd + "|" + tmpJd + "|"
				+ tmpWd + "|" + proType);
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
	public static List<String> filterList(List<String> list, String cell,
			String startTime, String endTime) {
		List<String> cellall = new ArrayList<String>();
		long start = Long.parseLong(startTime.replaceAll("[- :]", ""));
		long end = Long.parseLong(endTime.replaceAll("[- :]", ""));

		// 按照时间过滤小区
		for (int i = 0; i < list.size(); i++) {
			String[] s = pattern.split(list.get(i), -1);
			long time = Long.parseLong(s[18].replaceAll("[- :]", ""));
			if (s[7].equals(cell) && time >= start && time <= end) {
				cellall.add(list.get(i));
			}
		}
		return cellall;
	}


	// 查询数据
	public static List<String> startQuery_jl(String tableName, String start,
			String end, String[] msisdns) {
		long bef = System.currentTimeMillis();

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
				scan.setStartRow(getScanStartOrStop(start, msdn, (byte) '0'));
				scan.setStopRow(getScanStartOrStop(end, msdn, (byte) '9'));
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
		switch (table_index) {
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
		case 113:
			S1uGnrlZx svoz_gnrl=new S1uGnrlZx();
			list_zx=svoz_gnrl.ReturnListZx(list);
			break;
		case 114:
			S6aZx svoz_s6a=new S6aZx();
			list_zx=svoz_s6a.ReturnListZx(list);
			break;
		case 115:
			S1SgsZx svoz_sgs=new S1SgsZx();
			list_zx=svoz_sgs.ReturnListZx(list);
			break;
		default:
			break;
		}
		// 保证分页显示次序
		// Collections.sort(list);
		// 按照时间进行排序，http的详单结束位置在1位
		List<String> order = reverseIndex(list, 1);
		// 分页显示
		List<String> result = pageList(order, pageSize, index);
		//爱立信需求，第一个元素返回总条数
		result.add(0,String.valueOf(list.size()));
		return result;
	}
	
	
	//////////////////////////////////////////////////////////////////////////////////////////////////
	//吉林端到端添加
	public List<String> logQueryJl(int table_index,int cause, String start_time,String end_time,int pageSize, int index) {
		// 查询详单结果
		List<String> list;
		List<String> list2=new ArrayList<String>();
		String table_name=map_table.get(table_index);
		String cause_value= causelFormat(cause);
		String table_ind;

		list = getListJl(table_name,cause_value, start_time, end_time,1);
		log.info("flag==1的 list 大小："+list.size());
		if (list.size() == 0) {
			return new ArrayList<String>();	
		}
		//
		List<String> list_zx=null;
		switch (table_index) {
		case 116:
			S1uHttpCauseZx svoz_httpcsuse=new S1uHttpCauseZx();
			list_zx=svoz_httpcsuse.ReturnListZx(list);
			break;
		case 117:
			S1MmeCauseZx svoz_s1mmecause=new S1MmeCauseZx();
			list_zx=svoz_s1mmecause.ReturnListZx(list);
			break;
		default:
			break;
		}
		// 保证分页显示次序
		// Collections.sort(list);
		// 按照时间进行排序，http的详单结束位置在1位
		List<String> order = reverseIndex(list_zx, 1);
		// 分页显示
		List<String> result = pageList(order, pageSize, index);
		return result;
	}
	
		public static  String causelFormat(int cause)
		{	String cause_value_format="";
			String flag="0";
			String cause_value=String.valueOf(cause);
			if(cause_value.contains("-")){
				 flag="0";
			}else{
				flag="1";
			}
			cause_value=cause_value.replace("-", "");
			if(cause_value.length()==1){
				cause_value_format=flag+"0000"+cause_value;
			}else if(cause_value.length()==2){
				cause_value_format=flag+"000"+cause_value;
			}else if(cause_value.length()==3){
				cause_value_format=flag+"00"+cause_value;
			}else if(cause_value.length()==4){
				cause_value_format=flag+"0"+cause_value;
			}else if(cause_value.length()==5){
				cause_value_format=flag+cause_value;
			}
			return cause_value_format;
		}
		
		
		public static List<String> getListJl(String table_name,String cause_value, String startTime, String endTime,int flag) {
			List<String[]> tableDates = dateList(startTime, endTime);
			
			List<String> list = startQueryJl(table_name, cause_value, tableDates,flag);
			return list;
		}
	
		public static List<String> startQueryJl(String table, String cause_value, List<String[]> dates,int flag) {
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
						
					scan.setStartRow(getScanStartOrStopImsi(start, cause_value, (byte) '0'));
					scan.setStopRow(getScanStartOrStopImsi(end, cause_value, (byte) '9'));
					
					}
					else
					{
						scan.setStartRow(getScanStartOrStopCause(start, cause_value, (byte) '0'));
						scan.setStopRow(getScanStartOrStopCause(end, cause_value, (byte) '9'));
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
	
		public static byte[] getScanStartOrStopCause(String time, String cause, byte def) {
			int keyLength = 24;
			byte[] k = new byte[keyLength];
			for (int i = 0; i < k.length; i++) {
				k[i] = def;
			}
			int hashLen = 6;
			int dateLen = 14;
			byte[] hash = stringHashToBytes(cause, hashLen);
			byte[] date = formatFullLineDate(time, dateLen);
			System.arraycopy(hash, 0, k, 0, hashLen);
			System.arraycopy(date, 0, k, hashLen, dateLen);
			return k;
		}
	
	
	
	
	///////////////////////////////////////////////////////////////////////////////////////////////////
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
		List<String> list = getList(103,msisdn, startTime, endTime);
		List<String> list_23g = getList(112,msisdn, startTime, endTime);
		
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
			ul_data += Double.parseDouble(lines_23g[42]);
			dl_data += Double.parseDouble(lines_23g[43]);
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
		List<String> list = getList(103,msisdn, startTime, endTime);
		List<String> list_23g = getList(112,msisdn,startTime, endTime);
		
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
			Double data_23g = Double.parseDouble(lines_23g[42]) + Double.parseDouble(lines_23g[43]);
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
		//作为测试用
		
		List<String> list = startQuery(table_name, msisdn, tableDates,1);
		return list;
	}
	public static List<String> getList(String tab, String msisdns,
			String startTime, String endTime) {
		String day = startTime.replaceAll("[- :]", "").substring(0, 8);
		String table = tab + day;
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
				//测试表名
				//String tableName="O_RE_ST_XDR_PS_S1U_DNS_DETAIL20170401";
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
	protected String addDefault(String currentValue, String defaultValue) {
		return StringUtils.isBlank(currentValue) ? defaultValue : currentValue;
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

		 
		Endpoint.publish("http://" + args[0] + ":" + args[1] + "/QueryServer/QueryPort", new QuerySx());
		/*String msisdn="13689270000";
		String startTime="2016-07-24 00:20:49";
		String endTime="2016-07-24 23:25:49";*/
		//测试
//		QuerySx qSx=new QuerySx();
//		String msisdn="13488151000";
//		String startTime="2016-07-24 00:20:49";
//		String endTime="2016-07-24 23:25:49";
//		int table_flag=110;
//		int flag_index=2;
//		int pageSize=10;
//		int index=1;
//		int flag=1;
//		if(args.length==3)
//		{
//		table_flag=Integer.parseInt(args[0]);
//		msisdn=args[1];
//		startTime=args[2];
//		endTime=args[3];
//		pageSize=Integer.parseInt(args[4]);
//		index=Integer.parseInt(args[5]);
//		flag=Integer.parseInt(args[6]);
//		}	
//		List<String> list1=qSx.MmeUserGroup(msisdn,startTime,endTime);
//		for(String line:list1)
//			{
//			log.info(line);
//			}
//		List<String> list2=qSx.MmeUserFcauseGroup(msisdn,startTime,endTime);
//		for(String line:list2)
//			{
//			log.info(line);
//			}
		log.info("start success!");
		
	}
}
