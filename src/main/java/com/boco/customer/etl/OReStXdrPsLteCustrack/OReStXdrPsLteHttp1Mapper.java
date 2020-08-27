package com.boco.customer.etl.OReStXdrPsLteCustrack;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.boco.customer.field.S1uHttp;
import com.boco.customer.utils.DateUtils;
import com.boco.customer.utils.Metadata;
import com.boco.customer.utils.RedisUtils;


public class OReStXdrPsLteHttp1Mapper extends Mapper<Object, Text, Text, Text>  implements S1uHttp {

	private static Pattern pattern = Pattern.compile("\\|");
	public static Text keytext = new Text();
	public static Text valuetext = new Text();
	/*private int IMSI;
	private int MSISDN;
	private int TAC;
	private int ECI;
	private int STRAT_TIME;
	private int END_TIME;
	private int APP_TYPE;
	private int APP_SUB_TYPE;
	private int HTTP_WAP_STATUS;
	private int DL_DATA;*/

	public static Map<String, String> mDimSubapp = new HashMap<String, String>();
	String strDimSubapp;
	String[] arrDimSubapp;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		/*
		 Metadata rc;
		
		// 读取相应的初始化配置文件
		rc = new Metadata("/config/OReStXdrPsLteCustrackHttp.properties");
		IMSI = Integer.valueOf(rc.getValue("IMSI"));
		MSISDN = Integer.valueOf(rc.getValue("MSISDN"));
		TAC = Integer.valueOf(rc.getValue("TAC"));
		ECI = Integer.valueOf(rc.getValue("ECI"));
		STRAT_TIME = Integer.valueOf(rc.getValue("START_TIME"));
		END_TIME = Integer.valueOf(rc.getValue("END_TIME"));
		APP_TYPE = Integer.valueOf(rc.getValue("APP_TYPE"));
		APP_SUB_TYPE = Integer.valueOf(rc.getValue("APP_SUB_TYPE"));
		HTTP_WAP_STATUS = Integer.valueOf(rc.getValue("HTTP_WAP_STATUS"));
		DL_DATA = Integer.valueOf(rc.getValue("DL_DATA"));
		*/

		// 维表信息从redis读入
		mDimSubapp = RedisUtils.findCode2CodeIDMap("DIM_XDR_SUBAPP");

	}

	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] xdr = pattern.split(value.toString(), -1);
		//判断字段长度
		if (xdr.length < S1U_HTTP_LEN||!"451".endsWith(xdr[CITY])) {
			return;
		}
		int succ_sessions_cnt = 0;// 成功次数
		int att_cnt = 0;// 请求次数
		long business_time = 0;// 业务时长
		long dl_data = 0;// 下行流量
		String subapp_desc = "";

		// 获取二级业务名称
		strDimSubapp = mDimSubapp.get(xdr[APP_TYPE] + "|" + xdr[APP_SUB_TYPE]);
		if (StringUtils.isNotBlank(strDimSubapp)) {
			arrDimSubapp = pattern.split(strDimSubapp, -1);
			subapp_desc = arrDimSubapp[2];
		}

		// 成功次数
		try {
			if (Integer.valueOf(xdr[HTTP_WAP_STATUS]) >= 200
					&& Integer.valueOf(xdr[HTTP_WAP_STATUS]) < 400) {
				succ_sessions_cnt = 1;
			}
		} catch (Exception e) {
			// TODO: handle exception
			return;
		}
		// 连接次数
		if (Integer.valueOf(xdr[HTTP_WAP_STATUS]) > 0) {
			att_cnt = 1;
		}
		// 时长
		business_time = Long.parseLong(xdr[END_TIME])
				- Long.parseLong(xdr[START_TIME]);
		// 下行流量
		dl_data = StringUtils.isBlank(xdr[DL_DATA]) ? 0 : Long
				.parseLong(xdr[DL_DATA]);
		// 时间点
		String startTime = DateUtils.paserTime(xdr[START_TIME],
				"yyyy-MM-dd HH:mm:ss");// 开始时间YYYY-MM-DD HH24:MI:SS
		String endTime = DateUtils.paserTime(xdr[END_TIME],
				"yyyy-MM-dd HH:mm:ss");// 结束时间YYYY-MM-DD HH24:MI:SS
		// 组织key
		StringBuffer k = new StringBuffer();
		k.append(xdr[IMSI]).append("|");
		k.append(xdr[MSISDN]).append("|");
		k.append(xdr[TAC]).append("|");
		k.append(xdr[ECI]).append("|");
		k.append(startTime).append("|");
		k.append(endTime).append("|");
		k.append(subapp_desc).append("|");
		k.append("3").append("|"); // 维表中标识错误码类型ID
		k.append(xdr[HTTP_WAP_STATUS]);

		// 组织value
		StringBuffer v = new StringBuffer();
		v.append(succ_sessions_cnt).append("|");
		v.append(att_cnt).append("|");
		v.append(business_time).append("|");
		v.append(dl_data);
		// 输出到reducer
		keytext.set(k.toString());
		valuetext.set(v.toString());
		context.write(keytext, valuetext);
	}
}
