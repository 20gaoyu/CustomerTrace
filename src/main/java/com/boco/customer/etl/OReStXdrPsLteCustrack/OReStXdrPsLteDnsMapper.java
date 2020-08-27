package com.boco.customer.etl.OReStXdrPsLteCustrack;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.boco.customer.field.S1uDns;
import com.boco.customer.utils.DateUtils;
import com.boco.customer.utils.Metadata;

public class OReStXdrPsLteDnsMapper extends Mapper<Object, Text, Text, Text> implements S1uDns{

	private static Pattern pattern = Pattern.compile("\\|");
	public static Text keytext = new Text();
	public static Text valuetext = new Text();
	/*
	private int IMSI;
	
	private int MSISDN;
	private int TAC;
	private int ECI;
	private int START_TIME;
	private int END_TIME;
	private int DNS_RESP_CODE;
	private int DNS_REQ_CNT;
	private int DL_DATA;*/
	private static String procedure_type = "DNS查询";

	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		/*
		 Metadata rc;
		// 读取相应的初始化配置文件
		rc = new Metadata("/config/OReStXdrPsLteCustrackDns.properties");
		IMSI = Integer.valueOf(rc.getValue("IMSI"));
		MSISDN = Integer.valueOf(rc.getValue("MSISDN"));
		TAC = Integer.valueOf(rc.getValue("TAC"));
		ECI = Integer.valueOf(rc.getValue("ECI"));
		START_TIME = Integer.valueOf(rc.getValue("START_TIME"));
		END_TIME = Integer.valueOf(rc.getValue("END_TIME"));
		DNS_RESP_CODE = Integer.valueOf(rc.getValue("DNS_RESP_CODE"));
		DNS_REQ_CNT = Integer.valueOf(rc.getValue("DNS_REQ_CNT"));
		DL_DATA = Integer.valueOf(rc.getValue("DL_DATA"));
		*/
	}

	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] xdr = pattern.split(value.toString(), -1);
		// 判断长度
		if (xdr.length != 58||!"451".endsWith(xdr[CITY])) {
			return;
		}

		int succ_sessions_cnt = 0;// 成功次数
		int att_cnt = 0;// 请求次数
		long business_time = 0;// 业务时长

		// 成功次数
		if (xdr[DNS_RESP_CODE].equals("0")) {
			succ_sessions_cnt = 1;
		}
		// 连接次数
		att_cnt = StringUtils.isBlank(xdr[DNS_REQ_CNT]) ? 0 : Integer
				.valueOf(xdr[DNS_REQ_CNT]);
		// 时长
		business_time = Long.parseLong(xdr[END_TIME])
				- Long.parseLong(xdr[START_TIME]);
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
		k.append(procedure_type).append("|");
		k.append("2").append("|");// 维表中标识错误码类型ID，DNS的标识是2
		k.append(xdr[DNS_RESP_CODE]);

		// 组织value
		StringBuffer v = new StringBuffer();
		v.append(succ_sessions_cnt).append("|");
		v.append(att_cnt).append("|");
		v.append(business_time).append("|");
		v.append(StringUtils.isBlank(xdr[DL_DATA]) ? 0 : xdr[DL_DATA]);// 下行流量,mme无下行流量，标识dns
		// 输出到reducer
		keytext.set(k.toString());
		valuetext.set(v.toString());
		context.write(keytext, valuetext);
	}

}
