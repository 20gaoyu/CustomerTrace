package com.boco.customer.etl.OReStXdrPsLteCustrack;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.security.UserGroupInformation;

import com.boco.customer.hbase.HbaseCounter;
import com.boco.customer.hbase.KeyUtils;
import com.boco.customer.utils.HdfsToDim;
import com.boco.customer.utils.RedisUtils;


public class OReStXdrPsLteCustrackReducer extends
		Reducer<Text, Text, NullWritable, NullWritable> {
	public static Pattern pattern = Pattern.compile("\\|");
	private byte[] family;
	private byte[] column;
	private HTable table = null; // 基础汇总表
	Put put;
    Connection conn = null;
    private static Configuration conf;
    private BufferedMutator mutator;
    private List<Put> puts;
    private long i;
	private int errorcnt = 0; // 插入错误条数
	private int totalcnt = 0; // 总记录条数
	private int numerrorcnt = 0;// 号码有问题的记录数
	private String provinceName;

	// 小区
	public static Map<String, String> mDimNeEc = new HashMap<String, String>();
	String strDimNeEc;
	String[] arrDimNeEc;
	// HHTP和dns的错误码維表
	public static Map<String, String> mDimXdrCauseType = new HashMap<String, String>();
	String strDimXdrCauseType;
	String[] arrDimXdrCaseType;
	// mme的错误码维表
	public static Map<String, String> mDimXdrCauseMme = new HashMap<String, String>();
	// 黑龙江小区维表--用cgi关联，eci前5位转换为10进制
    public static Map<String, String> mDimNeEc_Hlj = new HashMap<String, String>();
    String strDimNeEc_Hlj;
    String[] dimNeEcArr_Hlj;
    
    String str_dim_xdr_cause_mme;
    String[] dim_xdr_cause_mme_arr;
    public static Map<String, String> dimXdrCause = new HashMap<String, String>();
	String strDimXdrCause;
	String[] dimXdrCauseArr;
	String kerberos_count=null;
	String if_kerberos = null;
	@SuppressWarnings("deprecation")
	@Override
	protected void setup(Context context) throws IOException {
		// 获取配置信息
		
		conf = context.getConfiguration();
		family = conf.get("table.family").getBytes();
		column = conf.get("table.column").getBytes();
		provinceName=conf.get("province.name");
		if_kerberos = conf.get("if_kerberos");
		kerberos_count = conf.get("kerberos_count");
		String tableName = conf.get("table.name");
		if("true".equals(if_kerberos))
		{
		System.out.println("REDUCE进行kerberos认证，账号："+kerberos_count);
		 //设置安全验证方式为kerberos
	    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(kerberos_count, "/home/boco/boco.keytab");
	    conn = ugi.doAs(new PrivilegedAction<Connection>() {
            @Override
            public Connection run() {
                try {
                    return ConnectionFactory.createConnection(conf);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }
        });
        //table = conn.getTable(TableName.valueOf(tableName));
        //table.setWriteBufferSize(12 * 1024 * 1024);
        mutator = conn.getBufferedMutator(TableName.valueOf(tableName));
        puts = new ArrayList<Put>();
        i = 0L;
		}

//		System.out.println("reduce新建表");
//		table = new HTable(conf, tableName);
//		// 设置自动刷出为false
//		table.setAutoFlush(false);
//		// 设置写入缓存
//		table.setWriteBufferSize(12 * 1024 * 1024);
		System.out.println("reduce加载维表");
		String valueEc="1,2,3,4,5,6,7,8,9,10,11,12,13,14,15";
		String valueTerm="1,2,3,4,5,6,7";
		String valueSubapp="1,2,3,4,5,6";
		String valueMmeip="1,2";
		String valueMmeCau="1,2";
		// 从redis中读取维度信息
		//mDimNeEc = RedisUtils.findCode2CodeIDMap("DIM_NE_EC");
		//mDimXdrCauseType = RedisUtils.findCode2CodeIDMap("DIM_XDR_CAUSETYPE");
		dimXdrCause = HdfsToDim.getDimInfo(conf, "/user/boco/dim/DIM_XDR_CAUSE_HTTP/", "0", valueMmeCau);
		mDimXdrCauseMme = HdfsToDim.getDimInfo(conf, "/user/boco/dim/DIM_XDR_CAUSE_MME/", "0", valueMmeCau);
		//黑龙江的的小区维表和一般的有区
		mDimNeEc_Hlj= HdfsToDim.getDimInfo(conf, "/user/boco/dim/DIM_NE_EC/", "0", valueEc);		
	}

	@SuppressWarnings("deprecation")
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		// 扩展属性
		String key_column = key.toString();
		String[] key_array = pattern.split(key_column, -1);
		String imsi = key_array[0];
		String msisdn = key_array[1];
		// 手机号码去掉+86开头的
		if(msisdn.startsWith("86"))
		{
			msisdn = msisdn.replaceFirst("86", "");
		}
		String tac = key_array[2];
		String eci = key_array[3];
		String start_time = key_array[4];
		String end_time = key_array[5];
		String procedure_type = key_array[6];
		String cause = key_array[7];
		String cause_value = key_array[8];

		// 从end_time获取day_id，hour_id
		String endTime = end_time.replaceAll("[- :]", "");
		String day_id = endTime.substring(0, 8);
		String hour_id = endTime.substring(8, 10);
		// 从tac，eci和维表关联出小区的相关信息
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
		strDimNeEc = mDimNeEc.get(tac + "|" + eci);
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
	    if(eci.length()>=7&&eci.length()<=9)
		{
	    try {
	    	strDimNeEc_Hlj=mDimNeEc_Hlj.get(handleEciHlj(eci));	
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
		// 从错误码维表得出错误描述信息
	    String cause_desc ="未知";
		if (cause.equals("9")) {// 9为mme
			str_dim_xdr_cause_mme = mDimXdrCauseMme.get(cause_value);
		    if(str_dim_xdr_cause_mme!=null){
		    	dim_xdr_cause_mme_arr=pattern.split(str_dim_xdr_cause_mme, -1);
			 cause_desc =dim_xdr_cause_mme_arr[0];
		    }
		} else {
			strDimXdrCause = dimXdrCause.get(cause_value + "");// CAUSE_VALUE	    
		    if(strDimXdrCause!=null){
		    	dimXdrCauseArr=pattern.split(strDimXdrCause, -1);
		    	cause_desc =dimXdrCauseArr[0];}
		}

		// 对数据进行汇总
		// 初始化性能指标
		long succ_sessions_cnt = 0;// 成功次数
		long att_cnt = 0;// 请求次数
		float business_delay = 0;// 业务时延
		long business_time = 0;// 业务时长
		long dl_data = 0;// 下行流量

		Iterator<Text> it = values.iterator();
		while (it.hasNext()) {
			String value_column = it.next().toString();
			String[] column = pattern.split(value_column, -1);
			succ_sessions_cnt += Integer.valueOf(column[0]);
			att_cnt += Integer.valueOf(column[1]);
			business_time += Long.parseLong(column[2]);
			dl_data += Long.parseLong(column[3]);
		}

		business_delay = att_cnt == 0 ? 0 : business_time / att_cnt;
		// 时间
		Date date = new Date();
		SimpleDateFormat foo = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String load_time = foo.format(date);
		// 拼接value
		StringBuffer sb = new StringBuffer();
		sb.append(day_id).append("|");
		sb.append(hour_id).append("|");
		sb.append(imsi).append("|");
		sb.append(msisdn).append("|");
		sb.append(tac).append("|");
		sb.append(eci).append("|");
		sb.append(ec_id).append("|");
		sb.append(ec_desc).append("|");
		sb.append(enb_id).append("|");
		sb.append(enb_desc).append("|");
		sb.append(city_id).append("|");
		sb.append(city_desc).append("|");
		sb.append(region_id).append("|");
		sb.append(region_desc).append("|");
		sb.append(covertype_desc).append("|");
		sb.append(longitude).append("|");
		sb.append(latitude).append("|");
		sb.append(start_time).append("|");
		sb.append(end_time).append("|");
		sb.append(procedure_type).append("|");
		sb.append(cause_value).append("|");
		sb.append(cause_desc).append("|");
		sb.append(succ_sessions_cnt).append("|");
		sb.append(att_cnt).append("|");
		sb.append(business_delay).append("|");
		sb.append(business_time).append("|");
		sb.append(dl_data).append("|");
		sb.append(load_time);

		totalcnt++;
		if (msisdn.length() < 11) {
			numerrorcnt++;
			return;
		} else {
			msisdn = msisdn.substring(0, 11);
		}
		// 组织key
		byte[] rowkey = KeyUtils.buildKey(msisdn, day_id, hour_id);
		// 向hbase中插入数据

//		try {
//			put = new Put(rowkey);
//			put.add(family, column, sb.toString().getBytes());
//			put.setWriteToWAL(false);
//			table.put(put);
//		} catch (IOException e) {
//			errorcnt++;
//			return;
//		}
	       Put put = new Put(rowkey);
	        put.setDurability(Durability.SKIP_WAL);
	        put.addColumn(family, column, sb.toString().getBytes());
	        i++;
	        puts.add(put);

	        if (i % 500 == 0) {
	            // System.out.println("i=" + i);
	            mutator.mutate(puts);
	            mutator.flush();
	            puts.clear();
	        }

	}

	/**
	 * 重写cleanup方法
	 */
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
//		try {
//			table.flushCommits();
//			table.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		 if (puts.size() > 0) {
	            System.out.println("size=" + puts.size());
	            mutator.mutate(puts);
	            mutator.flush();
	            puts.clear();
	        }

	        try {
	            mutator.close();
	            //table.close();
	            conn.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
		context.getCounter(HbaseCounter.LEN_ERR).increment(errorcnt);
		context.getCounter(HbaseCounter.ALL_RECORD).increment(totalcnt);
		context.getCounter(HbaseCounter.KEYWORD_ERR).increment(numerrorcnt);
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

