package com.boco.customer.etl.O_RE_ST_XDR_PS_S1U_IM;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.boco.customer.utils.IspUtil;
import com.boco.customer.field.S1uHttp;
import com.boco.customer.field.S1uIm;
import com.boco.customer.hbase.HbaseCounter;
import com.boco.customer.hbase.KeyUtils;
import com.boco.customer.utils.DateUtils;
import com.boco.customer.utils.RedisUtils;

public class OReStXdrPsS1uImMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> implements S1uIm {
	private static Pattern pattern = Pattern.compile("\\|");;// 分隔符
	private static Pattern pattern1 = Pattern.compile("\\.");;// 分隔符
	private static SimpleDateFormat foo = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private byte[] family;
	private byte[] column;
	private HTable table = null; // 基础汇总表
	private HTable indexTable = null; // 索引表
	private int totalrecord = 0;// 总记录数
	private int len_err = 0;// 字段长度不对记录数
	private int keyword_err = 0;// 关键字为空的记录数
	private int insert_err = 0;// 插入hbase失败记录数
	private String if_index = "false";// 是否需要索引

	public static Map<String, String> app_content_map = new HashMap<String, String>();
	static {
		app_content_map.put("1", "文本");
		app_content_map.put("2", "图片");
		app_content_map.put("3", "音频");
		app_content_map.put("4", "视频");
		app_content_map.put("5", "其他文件");
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
	Put put_index;
	@SuppressWarnings("deprecation")
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		// 获取配置信息
		Configuration conf = context.getConfiguration();
		String tableName = conf.get("table.name");
		String indexTableName = conf.get("table.index");
		family = conf.get("table.family").getBytes();
		column = conf.get("table.column").getBytes();
		table = new HTable(conf, tableName);
		indexTable = new HTable(conf, indexTableName);
		if_index=conf.get("table.if.index");
		// 设置自动刷出为false
		table.setAutoFlush(false);
		indexTable.setAutoFlush(false);
		// 设置写入缓存
		table.setWriteBufferSize(12 * 1024 * 1024);
		indexTable.setWriteBufferSize(12 * 1024 * 1024);
		// 读取维表信息
		mDimTermInf = RedisUtils.findCode2CodeIDMap("DIM_TERM_INF");
		mDimNeEc = RedisUtils.findCode2CodeIDMap("DIM_NE_EC");
		mDimSubapp = RedisUtils.findCode2CodeIDMap("DIM_XDR_SUBAPP");
		m_dim_ne_sgw = RedisUtils.findCode2CodeIDMap("DIM_NE_SGW");
		dimXdrCause = RedisUtils.findCode2CodeIDMap("DIM_XDR_CAUSE_HTTP");
	}

	@SuppressWarnings("deprecation")
	protected void map(LongWritable key, Text value, Context context) {
		String[] xdrdata = pattern.split(value.toString(), -1);
		totalrecord++;
		// 判断字段个数是否满足
		if (xdrdata.length < S1U_IM_LEN) {
			len_err++;//
			return;
		}
		// 手机号码去掉+86开头的
		String msisdn = xdrdata[MSISDN];
		String imsi = xdrdata[IMSI];
		msisdn = msisdn.replace("+86", "");
		if (msisdn.length() < 11) {
			keyword_err++;
			return;
		} else {
			msisdn = msisdn.substring(0, 11);
		}
		if (imsi.length()<15) {
			keyword_err++;
			return;
		} else {
			imsi=imsi.substring(0, 15);
		}
		// 时间处理
				String startTime = "";
				String endTime = "";
				String endTime_format="";
				String day_id="";
				String hour_id="";
				String min_id="";		
				try {
					startTime = DateUtils.paserTime(xdrdata[START_TIME], "yyyy-MM-dd HH:mm:ss.sss");
					
					endTime = DateUtils.paserTime(xdrdata[END_TIME], "yyyy-MM-dd HH:mm:ss.sss");
					endTime_format=DateUtils.paserTime(xdrdata[END_TIME]);
					 day_id = endTime_format.substring(0, 8);
					 hour_id = endTime_format.substring(8, 10);
					 min_id = endTime_format.substring(10, 12);
				} catch (Exception e) {
					keyword_err++;
					return;
				}
				
				// String day = endTime.replaceAll("[- :]", "").substring(0, 8);
				// String hour = endTime.replaceAll("[- :]", "").substring(8, 10);
				// 终端信息处理
				String vendor = ""; // 终端厂家
				String type_desc = "";// 终端型号
				String term_class = "";// 终端型号
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
//				String region_id = "";
				String region_desc = "";
			    String enb_id = "9999";
			    String enb_desc = "未知";
			    String covertype_desc="未知";
			    String longitude = "9999";
			    String latitude= "9999";
			    String vendor_id="9999";
			    String vendor_desc= "未知";
				strDimNeEc = mDimNeEc.get(xdrdata[TAC] + "|" + xdrdata[ECI]);
				if (StringUtils.isNotBlank(strDimNeEc)) {
					arrDimNeEc = pattern.split(strDimNeEc, -1);
					ec_id = arrDimNeEc[0];
					ec_desc = arrDimNeEc[1];
					city_id = arrDimNeEc[2];
					city_desc = arrDimNeEc[3];
					company_id = arrDimNeEc[4];
					company_desc = arrDimNeEc[5];
//					region_id = arrDimNeEc[6];
					region_desc = arrDimNeEc[7];
					enb_id = arrDimNeEc[8];
					enb_desc = arrDimNeEc[9];
					covertype_desc= arrDimNeEc[10];
					longitude = arrDimNeEc[11];
					latitude= arrDimNeEc[12];
					vendor_id= arrDimNeEc[13];
					vendor_desc= arrDimNeEc[14];
				}
				// 二级业务信息
						String subapp_key = "";
						String app_desc = "";
						String subapp_desc = "";
						String appflag_id = "";
						String appflag_desc = "";
						strDimSubapp = mDimSubapp.get(xdrdata[APP_TYPE] + "|" + xdrdata[APP_SUB_TYPE]);
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
				String sgw_vendor_id = "9999";
				String sgw_vendor_desc = "未知";
				String pgw_id = "9999";
				String pgw_desc = "未知";
				str_dim_ne_sgw = m_dim_ne_sgw.get(xdrdata[SGW_IP_ADD]);
				if (StringUtils.isNotBlank(str_dim_ne_sgw)) {
					dim_ne_sgw_arr = pattern.split(str_dim_ne_sgw, -1);
					sgw_id = dim_ne_sgw_arr[0];
					sgw_desc = dim_ne_sgw_arr[1];
					sgw_vendor_id= dim_ne_sgw_arr[2];
					sgw_vendor_desc= dim_ne_sgw_arr[3];
					pgw_id= dim_ne_sgw_arr[4];
					pgw_desc= dim_ne_sgw_arr[5];
				}
				// Serverip
				long isp_id = 0;
				String isp_desc = "";
				String isp_province_id = "";
				String isp_province_desc = "";
				if (StringUtils.isNotBlank(xdrdata[SERVER_IPV4])) {
					String[] ipArray = pattern1.split(xdrdata[SERVER_IPV4], -1);
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
				// 去xdr的city_id
				String region_id = xdrdata[CITY];
				// 入库时间
				String load_time = foo.format(new Date());
				// 拼接value的值
				StringBuffer sb = new StringBuffer();
				sb.append(day_id).append("|");
				sb.append(hour_id).append("|");
				sb.append(min_id).append("|");
				sb.append(endTime).append("|");
				sb.append(xdrdata[LENGTH]).append("|");
				sb.append(xdrdata[CITY]).append("|");
				sb.append(xdrdata[INTERFACE]).append("|");
				sb.append(xdrdata[XDR_ID]).append("|");
				sb.append(xdrdata[RAT]).append("|");
				sb.append(xdrdata[IMSI]).append("|");
				sb.append(xdrdata[IMEI]).append("|");
				sb.append(xdrdata[MSISDN]).append("|");
				sb.append(imei_tac).append("|");
				sb.append(vendor).append("|");
				sb.append(type_desc).append("|");
				sb.append(term_class).append("|");
				sb.append(3).append("|");
				sb.append(xdrdata[MACHINE_IP_ADD_TYPE]).append("|");
				sb.append(xdrdata[SGW_IP_ADD]).append("|");

				sb.append(sgw_id).append("|");
				sb.append(sgw_desc).append("|");
				sb.append(sgw_vendor_id).append("|");
				sb.append(sgw_vendor_desc).append("|");
				sb.append(pgw_id).append("|");
				sb.append(pgw_desc).append("|");
				sb.append(xdrdata[ENODEB_IP_ADD]).append("|");
				sb.append(xdrdata[SGW_PORT]).append("|");
				sb.append(xdrdata[ENODEB_PORT]).append("|");
				sb.append(xdrdata[ENB_GTP_TEID]).append("|");
				sb.append(xdrdata[SGW_GTP_TEID]).append("|");
				sb.append(xdrdata[APN]).append("|");
				sb.append(xdrdata[TAC]).append("|");
				sb.append(xdrdata[ECI]).append("|");
				sb.append(ec_id).append("|");
				sb.append(ec_desc).append("|");
				sb.append(enb_id ).append("|");
				sb.append(enb_desc).append("|");
				sb.append(city_id).append("|");
				sb.append(city_desc).append("|");
				sb.append(company_id).append("|");
				sb.append(company_desc).append("|");
				sb.append(region_id).append("|");
				sb.append(region_desc).append("|");

				sb.append(covertype_desc ).append("|");
				sb.append(longitude).append("|");
				sb.append(latitude).append("|");
				sb.append(vendor_id ).append("|");
				sb.append(vendor_desc).append("|");
				sb.append(xdrdata[APP_TYPE_CODE]).append("|"); 
				sb.append(xdrdata[START_TIME]).append("|"); 
				sb.append(xdrdata[END_TIME ]).append("|"); 
				
				sb.append(subapp_key).append("|");
				sb.append(xdrdata[APP_TYPE]).append("|");
				sb.append(app_desc).append("|");
				sb.append(xdrdata[APP_SUB_TYPE]).append("|");
				sb.append(subapp_desc).append("|");
				sb.append("").append("|");//subend_desc
				sb.append(appflag_id).append("|");
				sb.append(appflag_desc).append("|");
				sb.append(xdrdata[APP_CONTENT]).append("|");
				sb.append(app_content_map.get(xdrdata[APP_CONTENT])).append("|");
				
				 sb.append(xdrdata[ PROTOCOL_TYPE]).append("|");
				 sb.append(xdrdata[ APP_TYPE]).append("|");
				 sb.append(xdrdata[ APP_SUB_TYPE]).append("|");
				 sb.append(xdrdata[APP_CONTENT]).append("|");
				 sb.append(xdrdata[ APP_STATUS]).append("|");
				 sb.append(xdrdata[ USER_IPV4]).append("|");
				 sb.append(xdrdata[ USER_IPV6]).append("|");
				 sb.append(xdrdata[ USER_PORT]).append("|");
				 sb.append(xdrdata[ L4_TT]).append("|");
				 sb.append(xdrdata[ SERVER_IPV4]).append("|");
				 sb.append(xdrdata[ SERVER_IPV6]).append("|");
				 sb.append(xdrdata[ SERVER_PORT]).append("|");
				
				 sb.append(isp_id).append("|");
				sb.append(isp_desc).append("|");
				sb.append(isp_province_id).append("|");
				sb.append(isp_province_desc).append("|");
				sb.append(xdrdata[UL_DATA]).append("|");
				sb.append(xdrdata[DL_DATA]).append("|");
				sb.append(xdrdata[UL_IP_PACKET]).append("|");
				sb.append(xdrdata[DL_IP_PACKET]).append("|");
				sb.append(xdrdata[UL_TCP_DISCORDNUM]).append("|");
				sb.append(xdrdata[DL_TCP_DISCORDNUM]).append("|");
				sb.append(xdrdata[UL_TCP_RENUM]).append("|");
				sb.append(xdrdata[DL_TCP_RENUM]).append("|");
				sb.append(xdrdata[UL_IP_FRAG_PACKETS]).append("|");
				sb.append(xdrdata[DL_IP_FRAG_PACKETS]).append("|");
				sb.append(xdrdata[TCP_RESPONSE_DELAY]).append("|");
				sb.append(xdrdata[TCP_CONFIRM_DELAY]).append("|");
				sb.append(xdrdata[TCP_SUCC_REQUEST_DELAY]).append("|");
				sb.append(xdrdata[FIRST_RESPONSE_DELAY]).append("|");
				sb.append(xdrdata[WINDOW_SIZE]).append("|");
				sb.append(xdrdata[MSS_SIZE]).append("|");
				sb.append(xdrdata[TCP_ATT_CNT]).append("|");
				sb.append(xdrdata[TCP_CONN_STATUS]).append("|");
				sb.append(xdrdata[SESSION_MARK_END ]).append("|");
				

				sb.append(xdrdata[USERNAME]).append("|");
				sb.append(xdrdata[VERSION]).append("|");
				sb.append(xdrdata[CLIENT_TYPE]).append("|");
				sb.append(xdrdata[CONTENT_TYPE]).append("|");
				sb.append(load_time);
		// 组织key
		byte[] rowkey = KeyUtils.buildKey(msisdn, endTime_format.replaceAll("[- :]", ""));// 20160522152800
		// 向hbase中插入数据
		byte[] rowkey_index = KeyUtils.buildKey(imsi, endTime_format.replaceAll("[- :]", ""));// 20160522152800
		try {
			put = new Put(rowkey);
			put.add(family, column, sb.toString().getBytes());
			put.setWriteToWAL(false);
			table.put(put);
			if(if_index.equals("true")){
			put_index=new Put(rowkey_index);
			put_index.add(family, column, rowkey);
			put_index.setWriteToWAL(false);
			indexTable.put(put_index);
			}
		} catch (IOException e) {
			insert_err++;
		}
	}

	/**
	 * 重写cleanup方法
	 */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		try {

			table.flushCommits();
			table.close();
			if(if_index.equals("true")){
			indexTable.flushCommits();
			indexTable.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		context.getCounter(HbaseCounter.ALL_RECORD).increment(totalrecord);
		context.getCounter(HbaseCounter.LEN_ERR).increment(len_err);
		context.getCounter(HbaseCounter.KEYWORD_ERR).increment(keyword_err);
		context.getCounter(HbaseCounter.INSERT_ERR).increment(insert_err);
	}
}
