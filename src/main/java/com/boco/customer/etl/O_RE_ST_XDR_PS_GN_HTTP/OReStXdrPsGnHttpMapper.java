package com.boco.customer.etl.O_RE_ST_XDR_PS_GN_HTTP;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.boco.customer.utils.IspUtil;
import com.boco.customer.field.GnHttp;
import com.boco.customer.hbase.HbaseCounter;
import com.boco.customer.hbase.KeyUtils;
import com.boco.customer.utils.DateUtils;
import com.boco.customer.utils.HdfsToDim;
import com.boco.customer.utils.RedisUtils;
import com.boco.customer.webservice.QuerySx;

public class OReStXdrPsGnHttpMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> implements GnHttp {
	private static Pattern pattern = Pattern.compile("\\|");;// 分隔符
	private static Pattern pattern1 = Pattern.compile("\\.");;// 分隔符
	private static SimpleDateFormat foo = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	protected static final Logger log = Logger.getLogger(OReStXdrPsGnHttpMapper.class);
	private byte[] family;
	private byte[] column;
	private HTable table = null; // 基础汇总表
	private int totalrecord = 0;// 总记录数
	private int len_err = 0;// 字段长度不对记录数
	private int keyword_err = 0;// 关键字为空的记录数
	private int insert_err = 0;// 插入hbase失败记录数

	public static Map<String, String> app_content_map = new HashMap<String, String>();
	static {
		app_content_map.put("test", "文本");
		app_content_map.put("image", "图片");
		app_content_map.put("audio", "音频");
		app_content_map.put("video", "视频");
		app_content_map.put("application", "应用程序");
		app_content_map.put("9999", "未知");
	}

	// 终端
	public static Map<String, String> mDimTermInf = new HashMap<String, String>();
	String strDimTermInf;
	String[] arrDimTermInf;
	// 小区
	public static Map<String, String> mDimNeEc = new HashMap<String, String>();
	String strDimNeEc;
	String[] arrDimNeEc;
	// 二级业务
	public static Map<String, String> mDimSubapp = new HashMap<String, String>();
	String strDimSubapp;
	String[] arrDimSubapp;
	// SGW 维表
	public static Map<String, String> m_dim_ne_sgw = new HashMap<String, String>();
	String str_dim_ne_sgw;
	String[] dim_ne_sgw_arr;
	// 原因码维表 DIM_XDR_CAUSETYPE
	public static Map<String, String> dimXdrCause = new HashMap<String, String>();
	String strDimXdrCause;
	String[] dimXdrCauseArr;
	// ip信息
	String strDimServerIp;
	String[] arrDimServerIp;

	Put put;

	@SuppressWarnings("deprecation")
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		// 获取配置信息
		Configuration conf = context.getConfiguration();
		String tableName = conf.get("table.name");
		family = conf.get("table.family").getBytes();
		column = conf.get("table.column").getBytes();
//		table = new HTable(conf, tableName);
//		// 设置自动刷出为false
//		table.setAutoFlush(false);
//		// 设置写入缓存
//		table.setWriteBufferSize(12 * 1024 * 1024);
		// 读取维表信息
		String valueEc="1,2,3,4,5,6,7,8,9,10,11,12,13,14,15";
		String valueTerm="1,2,3,4,5,6,7";
		String valueSubapp="2,3,4,5,6";
		String valueSgwip="1,2,3,4,5,6";
		String valueHttpCau="1,2";
		mDimTermInf = HdfsToDim.getDimInfo(conf, "/user/boco/dim/DIM_TERM_INF/", "0", valueTerm);
		//mDimNeEc = HdfsToDim.getDimInfo(conf, "/user/boco/dim/DIM_NE_EC/", "0", valueEc);
		mDimSubapp =  HdfsToDim.getDimInfo(conf, "/user/boco/dim/DIM_XDR_SUBAPP/", "0,1", valueSubapp);
		m_dim_ne_sgw = HdfsToDim.getDimInfo(conf, "/user/boco/dim/DIM_NE_SGW_IP/", "0", valueSgwip);
		dimXdrCause = HdfsToDim.getDimInfo(conf, "/user/boco/dim/DIM_XDR_CAUSE_HTTP/", "0", valueHttpCau);
		//黑龙江的的小区维表和一般的有区别
		mDimNeEc = HdfsToDim.getDimInfo(conf, "/user/boco/dim/DIM_NE_EC/", "0", valueEc);	
	}

	@SuppressWarnings("deprecation")
	protected void map(LongWritable key, Text value, Context context) {
		String[] xdrdata = pattern.split(value.toString(), -1);
		totalrecord++;
		//log.info("xdrdata.length："+xdrdata.length);
		// 判断字段个数是否满足
		if (xdrdata.length != GN_HTTP_LEN) {
			len_err++;//
			return;
		}
		// 手机号码去掉+86开头的
		String msisdn = xdrdata[MSISDN];
		msisdn = msisdn.replace("+86", "");
		// 手机号码不足11位的
		if (msisdn.length() < 11) {
			keyword_err++;
			return;
		} else {
			msisdn = msisdn.substring(0, 11);
		}
		// 业务处理
		// 时间处理
		String startTime = "";
		String endTime = "";
		try {
			startTime = DateUtils.paserTime(xdrdata[START_TIME], "yyyy-MM-dd HH:mm:ss.sss");
			
			endTime = DateUtils.paserTime(xdrdata[END_TIME], "yyyy-MM-dd HH:mm:ss.sss");
		} catch (Exception e) {
			keyword_err++;
			return;
		}
		// String day = endTime.replaceAll("[- :]", "").substring(0, 8);
		// String hour = endTime.replaceAll("[- :]", "").substring(8, 10);
		// 终端信息处理
		String vendor = ""; // 终端厂家
		String type_desc = "";// 终端型号
		String imei_tac = xdrdata[IMEI].length() >= 8 ? xdrdata[IMEI].substring(0, 8) : xdrdata[IMEI];
		strDimTermInf = mDimTermInf.get(imei_tac);
		if (StringUtils.isNotBlank(strDimTermInf)) {
			arrDimTermInf = pattern.split(strDimTermInf, -1);
			vendor = arrDimTermInf[0];
			type_desc = arrDimTermInf[1];
		}
		// 小区信息
		String ec_id = "";
		String ec_desc = "";
		String city_id = "";
		String city_desc = "";
		String company_id = "";
		String company_desc = "";
		String region_id = "";
		String region_desc = "";
		strDimNeEc = mDimNeEc.get(xdrdata[TAC] + "|" + xdrdata[CID]);
		if (StringUtils.isNotBlank(strDimNeEc)) {
			arrDimNeEc = pattern.split(strDimNeEc, -1);
			ec_id = arrDimNeEc[0];
			ec_desc = arrDimNeEc[1];
			city_id = arrDimNeEc[2];
			city_desc = arrDimNeEc[3];
			company_id = arrDimNeEc[4];
			company_desc = arrDimNeEc[5];
			region_id = arrDimNeEc[6];
			region_desc = arrDimNeEc[7];
		}
		// 二级业务信息
		String subapp_key = "";
		String app_desc = "";
		String subapp_desc = "";
		String appflag_id = "";
		String appflag_desc = "";
		strDimSubapp = mDimSubapp.get(xdrdata[PROT_CATEGORY] + "|" + xdrdata[PROT_TYPE]);
		if (StringUtils.isNotBlank(strDimSubapp)) {
			arrDimSubapp = pattern.split(strDimSubapp, -1);
			subapp_key = arrDimSubapp[0];
			app_desc = arrDimSubapp[1];
			subapp_desc = arrDimSubapp[2];
			appflag_id = arrDimSubapp[3];
			appflag_desc = arrDimSubapp[4];
		}
		String sgw_id = "9999";
		String sgw_desc = "未知";
		str_dim_ne_sgw = m_dim_ne_sgw.get(xdrdata[GGSN_U_IP]);
		if (StringUtils.isNotBlank(str_dim_ne_sgw)) {
			dim_ne_sgw_arr = pattern.split(str_dim_ne_sgw, -1);
			sgw_id = dim_ne_sgw_arr[0];
			sgw_desc = dim_ne_sgw_arr[1];
		}
		// Serverip
		long isp_id = 0;
		String isp_desc = "";
		String isp_province_id = "";
		String isp_province_desc = "";
		if (StringUtils.isNotBlank(xdrdata[SERVER_IP])) {
			String[] ipArray = pattern1.split(xdrdata[SERVER_IP], -1);
			// 经常出现xdr传入的ip解析错误，为避免程序运行错误，加判断
			if (ipArray.length == 4) {
				isp_id = Long.parseLong(ipArray[0]) * 256 * 256 * 256 + Long.parseLong(ipArray[1]) * 256 * 256
						+ Long.parseLong(ipArray[2]) * 256 + Long.parseLong(ipArray[3]);
			}
		}
		strDimServerIp = IspUtil.getIspByEndIP(isp_id);
		arrDimServerIp = pattern.split(strDimServerIp, -1);
		isp_desc = arrDimServerIp[1];
		isp_province_id = arrDimServerIp[2];
		isp_province_desc = arrDimServerIp[3];
		//HTTP_CONTENT_TYPE
		String CONTENT_TYPE="";
		if(xdrdata[HTTP_CONTENT_TYPE].equals("test"))	
		{
			CONTENT_TYPE="test";
		}	
		else if (xdrdata[HTTP_CONTENT_TYPE].equals("image"))	
		{
			CONTENT_TYPE="image";
		}
		else if (xdrdata[HTTP_CONTENT_TYPE].equals("audio"))	
		{
			CONTENT_TYPE="audio";
		}
		else if (xdrdata[HTTP_CONTENT_TYPE].equals("video"))	
		{
			CONTENT_TYPE="video";
		}
		else if (xdrdata[HTTP_CONTENT_TYPE].equals("application"))	
		{
			CONTENT_TYPE="application";
		}
		else {
			CONTENT_TYPE="9999";
		}
		// 入库时间
		String load_time = foo.format(new Date());
		// 拼接value的值
		StringBuffer sb = new StringBuffer();
		sb.append(startTime).append("|");
		sb.append(endTime).append("|");
		sb.append(xdrdata[IMSI]).append("|");
		sb.append(xdrdata[IMEI]).append("|");//3
		sb.append(xdrdata[MSISDN]).append("|");
		sb.append(imei_tac).append("|");//5
		sb.append(vendor).append("|");//6
		sb.append(type_desc).append("|");//7
		sb.append(xdrdata[RAT]).append("|");//8
		sb.append(xdrdata[APN]).append("|");//9
		sb.append(xdrdata[TAC]).append("|");
		sb.append(xdrdata[CID]).append("|");
		sb.append(ec_id).append("|");
		sb.append(ec_desc).append("|");
		sb.append(city_id).append("|");
		sb.append(city_desc).append("|");
		sb.append(company_id).append("|");
		sb.append(company_desc).append("|");
		sb.append(region_id).append("|");
		sb.append(region_desc).append("|");
		sb.append(subapp_key).append("|");
		sb.append(xdrdata[PROT_CATEGORY]).append("|");
		sb.append(app_desc).append("|");//22
		sb.append(xdrdata[PROT_TYPE]).append("|");
		sb.append(subapp_desc).append("|");//24
		sb.append(appflag_id).append("|");
		sb.append(appflag_desc).append("|");
		sb.append(xdrdata[HTTP_CONTENT_TYPE]).append("|");
		sb.append(app_content_map.get(CONTENT_TYPE)).append("|");
		sb.append(sgw_id).append("|");
		sb.append(sgw_desc).append("|");
		sb.append(xdrdata[MS_PORT]).append("|");
		sb.append(arrDimServerIp[0]).append("|");
		sb.append(isp_desc).append("|");
		sb.append(isp_province_id).append("|");
		sb.append(isp_province_desc).append("|");
		sb.append(xdrdata[SERVER_IP]).append("|");
		sb.append(xdrdata[TRANS_RSP_CODE]).append("|");
		sb.append(dimXdrCause.get(xdrdata[TRANS_RSP_CODE])).append("|");
		sb.append(xdrdata[HOST]).append("|");
		sb.append(xdrdata[URL]).append("|");//40
		sb.append(xdrdata[L4_UL_THROUGHPUT]).append("|");//41
		sb.append(xdrdata[L4_DW_THROUGHPUT]).append("|");//42
		sb.append(xdrdata[L4_UL_GOODPUT]).append("|");
		sb.append(xdrdata[L4_DW_GOODPUT]).append("|");
		sb.append(xdrdata[TCP_WTP_UL_OUTOFSEQU]).append("|");
		sb.append(xdrdata[TCP_WTP_DW_OUTOFSEQU]).append("|");
		sb.append(xdrdata[TCP_WTP_UL_RETRANS]).append("|");
		sb.append(xdrdata[TCP_WTP_DW_RETRANS]).append("|");
		sb.append(xdrdata[UL_IP_FRAG_PACKETS]).append("|");
		sb.append(xdrdata[DL_IP_FRAG_PACKETS]).append("|");
//		sb.append(xdrdata[TCP_RESPONSE_DELAY]).append("|");
//		sb.append(xdrdata[TCP_CONFIRM_DELAY]).append("|");
//		sb.append(xdrdata[TCP_SUCC_REQUEST_DELAY]).append("|");
//		sb.append(xdrdata[FIRST_RESPONSE_DELAY]).append("|");
//		sb.append(xdrdata[TCP_ATT_CNT]).append("|");
		sb.append(xdrdata[TCP_RTT_STEP1]).append("|"); //TCP_RESPONSE_DELAY
		sb.append(xdrdata[TCP_RTT]).append("|"); //TCP_CONFIRM_DELAY
		sb.append(xdrdata[MS_ACK_TO_1STGET_DELAY]).append("|"); //TCP_SUCC_REQUEST_DELAY
		sb.append(xdrdata[FRISTTRANSREPTORSPDELAY]).append("|"); //FIRST_RESPONSE_DELAY
		sb.append(xdrdata[TCP_CONN_TIMES]).append("|"); //TCP_ATT_CNT
		sb.append(xdrdata[FST_HTTP_DELAY]).append("|");
		sb.append(xdrdata[TRANSACTION_DELAY]).append("|");
		sb.append(xdrdata[LAST_ACK_DELAY]).append("|");
		sb.append(load_time);
		// 组织key
		byte[] rowkey = KeyUtils.buildKey(msisdn, endTime.replaceAll("[- :]", ""));// 20160522152800
		// 向hbase中插入数据
		try {
			ImmutableBytesWritable k = new ImmutableBytesWritable(rowkey);
		    KeyValue kvProtocol = new KeyValue(rowkey, family, column, sb.toString().getBytes());
			context.write(k, kvProtocol);
		} catch (Exception e) {
			insert_err++;
		}
	}

	/**
	 * 重写cleanup方法
	 */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
//		try {
//
//			table.flushCommits();
//			table.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		context.getCounter(HbaseCounter.ALL_RECORD).increment(totalrecord);
		context.getCounter(HbaseCounter.LEN_ERR).increment(len_err);
		context.getCounter(HbaseCounter.KEYWORD_ERR).increment(keyword_err);
		context.getCounter(HbaseCounter.INSERT_ERR).increment(insert_err);
	}
}
