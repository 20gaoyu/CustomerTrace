package com.boco.customer.etl.O_RE_ST_XDR_PS_S1MME;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.boco.customer.utils.IspUtil;
import com.boco.customer.field.S1Mme;
import com.boco.customer.hbase.HbaseCounter;
import com.boco.customer.hbase.KeyUtils;
import com.boco.customer.utils.DateUtils;
import com.boco.customer.utils.HdfsToDim;
import com.boco.customer.utils.RedisUtils;
import org.apache.commons.logging.Log;

public class OReStXdrPsS1uMmeMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> implements S1Mme {
	private static Pattern pattern = Pattern.compile("\\|");;// 分隔符
	private static Pattern pattern1 = Pattern.compile("\\.");;// 分隔符
	private static SimpleDateFormat foo = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static final Log log = LogFactory.getLog(OReStXdrPsS1uMmeMapper.class);
	
	private byte[] family;
	private byte[] column;
	private HTable table = null; // 基础汇总表
	private HTable indexTable = null; // 索引表
	private int totalrecord = 0;// 总记录数
	private int len_err = 0;// 字段长度不对记录数
	private int keyword_err = 0;// 关键字为空的记录数
	private int insert_err = 0;// 插入hbase失败记录数
	private String provinceName;
	private String if_index = "false";// 是否需要索引
	private String errReasonString=""; 

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
	  // mme 维表
    public static Map<String, String> m_dim_ne_mme = new HashMap<String, String>();
    String str_dim_ne_mme;
    String[] dim_ne_mme_arr;
	// mme 原因码
    public static Map<String, String> m_dim_xdr_cause_mme = new HashMap<String, String>();
    String str_dim_xdr_cause_mme;
    String[] dim_xdr_cause_mme_arr;
    // 黑龙江小区维表--用cgi关联，eci前5位转换为10进制
    public static Map<String, String> mDimNeEc_Hlj = new HashMap<String, String>();
    String strDimNeEc_Hlj;
    String[] dimNeEcArr_Hlj;
    
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
		provinceName=conf.get("province.name");
		String indexTableName = conf.get("table.index");
		
		family = conf.get("table.family").getBytes();
		column = conf.get("table.column").getBytes();
//		table = new HTable(conf, tableName);
//		indexTable = new HTable(conf, indexTableName);
//		if_index=conf.get("table.if.index");
//		// 设置自动刷出为false
//		table.setAutoFlush(false);
//		indexTable.setAutoFlush(false);
//		// 设置写入缓存
//		table.setWriteBufferSize(12 * 1024 * 1024);
//		indexTable.setWriteBufferSize(12 * 1024 * 1024);
		// 读取维表信息
		String valueEc="1,2,3,4,5,6,7,8,9,10,11,12,13,14,15";
		String valueTerm="1,2,3,4,5,6,7";
		String valueSubapp="1,2,3,4,5,6";
		String valueMmeip="1,2";
		String valueMmeCau="1,2";
		mDimTermInf = HdfsToDim.getDimInfo(conf, "/user/boco/dim/DIM_TERM_INF/", "0", valueTerm);
		//mDimNeEc = HdfsToDim.getDimInfo(conf, "/user/boco/dim/DIM_NE_EC/", "0", valueEc);
		mDimSubapp =  HdfsToDim.getDimInfo(conf, "/user/boco/dim/DIM_XDR_SUBAPP/", "0", valueSubapp);
		m_dim_ne_mme = HdfsToDim.getDimInfo(conf, "/user/boco/dim/DIM_NE_MME_IP/", "0", valueMmeip);
		m_dim_xdr_cause_mme = HdfsToDim.getDimInfo(conf, "/user/boco/dim/DIM_XDR_CAUSE_MME/", "0", valueMmeCau);
		//黑龙江的的小区维表和一般的有区别
		mDimNeEc = HdfsToDim.getDimInfo(conf, "/user/boco/dim/DIM_NE_EC/", "0", valueEc);
		mDimNeEc_Hlj= HdfsToDim.getDimInfo(conf, "/user/boco/dim/DIM_NE_EC/", "0", valueEc);
		System.out.println("维表加载完成");
	}

	@SuppressWarnings("deprecation")
	protected void map(LongWritable key, Text value, Context context) {
		String[] xdrdata = pattern.split(value.toString(), -1);
		totalrecord++;
		// 判断字段个数是否满足
		if (xdrdata.length < S1MME_LEN) {
			len_err++;//
			return;
		}
		// 手机号码去掉+86开头的
		String msisdn = xdrdata[MSISDN];
		String imsi = xdrdata[IMSI];
		if(msisdn.startsWith("86"))
		{
			msisdn = msisdn.replaceFirst("86", "");
		}
		// 手机号码不足11位的
		if (msisdn.length() < 11) {
			keyword_err++;
			errReasonString=errReasonString+"号码错误"+msisdn;
			return;
		} else {
			msisdn = msisdn.substring(0, 11);
		}
//		if (imsi.length()<15) {
//			keyword_err++;
//			return;
//		} else {
//			imsi=imsi.substring(0, 15);
//		}
		//原因码
		str_dim_xdr_cause_mme = m_dim_xdr_cause_mme.get(xdrdata[FAILURE_CAUSE]);
	    String cause_desc ="未知";
	    String cause_may ="未知";
	    
	    if(str_dim_xdr_cause_mme!=null){
	    	dim_xdr_cause_mme_arr=pattern.split(str_dim_xdr_cause_mme, -1);
		 cause_desc =dim_xdr_cause_mme_arr[0];
		 if(dim_xdr_cause_mme_arr.length==2) 
			 cause_may=dim_xdr_cause_mme_arr[1];

	    }
	 // 业务处理
	 		// 时间处理
		String startTime = "";
		String endTime = "";
		String endTime_format="";
		String day_id="";
		String hour_id="";
		String min_id="";		
		try {
			startTime = DateUtils.paserTime(xdrdata[PROCEDURE_STRAT_TIME], "yyyy-MM-dd HH:mm:ss.sss");
			
			endTime = DateUtils.paserTime(xdrdata[PROCEDURE_END_TIME], "yyyy-MM-dd HH:mm:ss.sss");
			endTime_format=DateUtils.paserTime(xdrdata[PROCEDURE_END_TIME]);
			 day_id = endTime_format.substring(0, 8);
			 hour_id = endTime_format.substring(8, 10);
			 min_id = endTime_format.substring(10, 12);
		} catch (Exception e) {
			keyword_err++;
			errReasonString=errReasonString+"时间错误"+startTime;
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
		String ec_id = "9999";
		String ec_desc = "未知";
		String city_id = "9999";
		String city_desc = "未知";
		String company_id = "9999";
		String company_desc = "未知";
		String region_id = "9999";
		String region_desc = "未知";
	    String enb_id = "9999";
	    String enb_desc = "未知";
	    String covertype_desc="未知";
	    String longitude = "9999";
	    String latitude= "9999";
	    String vendor_id="9999";
	    String vendor_desc= "未知";
	    if(!provinceName.equals("HEILONGJIANG")) {
		strDimNeEc = mDimNeEc.get(xdrdata[TAC] + "|" + xdrdata[CELL_ID]);
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
			enb_id = arrDimNeEc[8];
			enb_desc = arrDimNeEc[9];
			covertype_desc= arrDimNeEc[10];
			longitude = arrDimNeEc[11];
			latitude= arrDimNeEc[12];
			vendor_id= arrDimNeEc[13];
			vendor_desc= arrDimNeEc[14];
		}
	    }
	    //黑龙江的小区特殊处理
	    if(provinceName.equals("HEILONGJIANG")) {
	    if(xdrdata[CELL_ID].length()>=7&&xdrdata[CELL_ID].length()<=9)
		{
	    try {
	    	strDimNeEc_Hlj=mDimNeEc_Hlj.get(handleEciHlj(xdrdata[CELL_ID]));
		} catch (Exception e) {
			// TODO: handle exception
			return;
		}
		if (StringUtils.isNotBlank(strDimNeEc_Hlj)) {
			dimNeEcArr_Hlj = pattern.split(strDimNeEc_Hlj + "|w");
		    ec_id=dimNeEcArr_Hlj[0];
		    ec_desc=dimNeEcArr_Hlj[1];
		    city_id=dimNeEcArr_Hlj[2];
		    city_desc=dimNeEcArr_Hlj[3];
		    company_id=dimNeEcArr_Hlj[4];
		    company_desc=dimNeEcArr_Hlj[5];
		    region_id=dimNeEcArr_Hlj[6];
		    region_desc=dimNeEcArr_Hlj[7];
		    enb_id=dimNeEcArr_Hlj[8];
			enb_desc=dimNeEcArr_Hlj[9];
			covertype_desc=dimNeEcArr_Hlj[10];
			longitude=dimNeEcArr_Hlj[11];
			latitude=dimNeEcArr_Hlj[12];
			vendor_id = dimNeEcArr_Hlj[13];
			vendor_desc = dimNeEcArr_Hlj[14];
		}
		}
	    }
		str_dim_ne_mme =m_dim_ne_mme.get(xdrdata[MME_IP_ADD].trim());
	    String mme_id="";
	    String mme_desc="";
	    if(StringUtils.isNotBlank(str_dim_ne_mme)){
		dim_ne_mme_arr= pattern.split(str_dim_ne_mme + "|w");
		mme_id=dim_ne_mme_arr[0];
		mme_desc=dim_ne_mme_arr[1];
	    }
	   // String region_id = xdrdata[CITY];
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
		sb.append(msisdn).append("|");
		sb.append(imei_tac).append("|");
		sb.append(vendor).append("|");
		sb.append(type_desc).append("|");
		sb.append(3).append("|");
		sb.append(xdrdata[PROCEDURE_TYPE]).append("|");
		sb.append(xdrdata[PROCEDURE_STRAT_TIME]).append("|");
		sb.append(xdrdata[PROCEDURE_END_TIME]).append("|");
		sb.append(xdrdata[PROCEDURE_STATUS]).append("|");
		sb.append(xdrdata[REQUEST_CAUSE]).append("|");
		sb.append(xdrdata[FAILURE_CAUSE]).append("|");
		sb.append(cause_desc ).append("|");//CAUSE_DESC
		sb.append(cause_may ).append("|");
		sb.append(xdrdata[KEYWORD1]).append("|");
		sb.append(xdrdata[KEYWORD2]).append("|");
		sb.append(xdrdata[KEYWORD3]).append("|");
		sb.append(xdrdata[KEYWORD4]).append("|");
		sb.append(xdrdata[MME_UE_S1AP_ID]).append("|");
		sb.append(xdrdata[OLD_MME_GROUP_ID]).append("|");
		sb.append(xdrdata[OLD_MME_CODE]).append("|");
		sb.append(xdrdata[OLDM_TMSI]).append("|");
		sb.append(xdrdata[MME_GROUP_ID]).append("|");
		sb.append(xdrdata[MME_CODE]).append("|");
		sb.append(xdrdata[M_TMSI]).append("|");
		sb.append(xdrdata[UE_IP_ADD_TYPE]).append("|");
		sb.append(xdrdata[USER_IP]).append("|");
		sb.append(xdrdata[MACHINE_IP_ADD_TYPE]).append("|");
		sb.append(xdrdata[MME_IP_ADD]).append("|");
		sb.append(mme_id     ).append("|");//MME_ID
		sb.append(mme_desc   ).append("|");//MME_DESC
		sb.append(xdrdata[ENB_IP_ADD]).append("|");
		sb.append(xdrdata[MME_PORT]).append("|");
		sb.append(xdrdata[ENB_PORT]).append("|");
		sb.append(xdrdata[TAC]).append("|");
		sb.append(xdrdata[CELL_ID]).append("|");
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
		sb.append(xdrdata[OTHER_TAC ]).append("|");     
		sb.append(xdrdata[OTHER_ECI ]).append("|");     
		sb.append(xdrdata[APN ]).append("|");           
		sb.append(xdrdata[EPS_BEARER_CNT ]).append("|");

		sb.append(validXdrData(BEARER1ID          ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER1TYPE        ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER1QCI         ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER1STATUS      ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER1REQUESTCAUSE,xdrdata)).append("|");
		sb.append(validXdrData(BEARER1FAILURECAUSE,xdrdata)).append("|");
		sb.append(validXdrData(BEARER1ENBGTP_TEID ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER1SGWGTP_TEID ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER2ID          ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER2TYPE        ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER2QCI         ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER2STATUS      ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER2REQUESTCAUSE,xdrdata)).append("|");
		sb.append(validXdrData(BEARER2FAILURECAUSE,xdrdata)).append("|");
		sb.append(validXdrData(BEARER2ENBGTP_TEID ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER2SGWGTP_TEID ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER3ID          ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER3TYPE        ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER3QCI         ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER3STATUS      ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER3REQUESTCAUSE,xdrdata)).append("|");
		sb.append(validXdrData(BEARER3FAILURECAUSE,xdrdata)).append("|");
		sb.append(validXdrData(BEARER3ENBGTP_TEID ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER3SGWGTP_TEID ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER4ID          ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER4TYPE        ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER4QCI         ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER4STATUS      ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER4REQUESTCAUSE,xdrdata)).append("|");
		sb.append(validXdrData(BEARER4FAILURECAUSE,xdrdata)).append("|");
		sb.append(validXdrData(BEARER4ENBGTP_TEID ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER4SGWGTP_TEID ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER5ID          ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER5TYPE        ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER5QCI         ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER5STATUS      ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER5REQUESTCAUSE,xdrdata)).append("|");
		sb.append(validXdrData(BEARER5FAILURECAUSE,xdrdata)).append("|");
		sb.append(validXdrData(BEARER5ENBGTP_TEID ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER5SGWGTP_TEID ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER6ID          ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER6TYPE        ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER6QCI         ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER6STATUS      ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER6REQUESTCAUSE,xdrdata)).append("|");
		sb.append(validXdrData(BEARER6FAILURECAUSE,xdrdata)).append("|");
		sb.append(validXdrData(BEARER6ENBGTP_TEID ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER6SGWGTP_TEID ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER7ID          ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER7TYPE        ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER7QCI         ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER7STATUS      ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER7REQUESTCAUSE,xdrdata)).append("|");
		sb.append(validXdrData(BEARER7FAILURECAUSE,xdrdata)).append("|");
		sb.append(validXdrData(BEARER7ENBGTP_TEID ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER7SGWGTP_TEID ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER8ID          ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER8TYPE        ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER8QCI         ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER8STATUS      ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER8REQUESTCAUSE,xdrdata)).append("|");
		sb.append(validXdrData(BEARER8FAILURECAUSE,xdrdata)).append("|");
		sb.append(validXdrData(BEARER8ENBGTP_TEID ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER8SGWGTP_TEID ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER9ID          ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER9TYPE        ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER9QCI         ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER9STATUS      ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER9REQUESTCAUSE,xdrdata)).append("|");
		sb.append(validXdrData(BEARER9FAILURECAUSE,xdrdata)).append("|");
		sb.append(validXdrData(BEARER9ENBGTP_TEID ,xdrdata)).append("|");
		sb.append(validXdrData(BEARER9SGWGTP_TEID ,xdrdata)).append("|"); 
		sb.append(load_time);
		// 组织key
		byte[] rowkey = KeyUtils.buildKey(msisdn, endTime_format.replaceAll("[- :]", ""));// 20160522152800
		// 向hbase中插入数据
		byte[] rowkey_index = KeyUtils.buildKey(imsi, endTime_format.replaceAll("[- :]", ""));// 20160522152800
		try {
			ImmutableBytesWritable k = new ImmutableBytesWritable(rowkey);
		    KeyValue kvProtocol = new KeyValue(rowkey, family, column, sb.toString().getBytes());
			context.write(k, kvProtocol);
		
		} catch (Exception e) {
			insert_err++;
		}
	}
	
	protected  String validXdrData(int arrXdrDataNum,String[] xdrdata) {
    	if(arrXdrDataNum < xdrdata.length){
    		return xdrdata[arrXdrDataNum];
    	}else{    	
    	return null;
    	}
        }
 
	protected  String addDefault(String currentValue, String defaultValue) {
    	return StringUtils.isBlank(currentValue) ? defaultValue : currentValue;
        }
	/**
	 * 重写cleanup方法
	 */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("map完成");
		context.getCounter(HbaseCounter.ALL_RECORD).increment(totalrecord);
		context.getCounter(HbaseCounter.LEN_ERR).increment(len_err);
		context.getCounter(HbaseCounter.KEYWORD_ERR).increment(keyword_err);
		context.getCounter(HbaseCounter.INSERT_ERR).increment(insert_err);
	
	}
	
	public static  String handleEciHlj(String eci)
	{
		String s_="460-00-";
		int i=0;
		int j=0;
		String enb_id=null;
		String cell_id=null;
		if("0".equals(eci.substring(0, 1)))
		{
			enb_id=eci.substring(1, 6);
			//System.out.println(enb_id);
			i = Integer.parseInt(enb_id, 16);
			 cell_id=eci.substring(6, eci.length());
			j=Integer.parseInt(cell_id, 16);
		}
		else {
			enb_id=eci.substring(0, 5);
			//System.out.println(enb_id);
			i = Integer.parseInt(enb_id, 16);
			 cell_id=eci.substring(5, eci.length());
			j=Integer.parseInt(cell_id, 16);
		}

		String cgi=s_+i+"-"+j;
		//System.out.println("j"+j+"i"+i+"cgi:"+cgi);
		return cgi;
	}
	 
}
