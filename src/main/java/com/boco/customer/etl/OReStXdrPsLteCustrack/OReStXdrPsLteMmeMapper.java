package com.boco.customer.etl.OReStXdrPsLteCustrack;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.boco.customer.field.S1Mme;
import com.boco.customer.utils.DateUtils;
import com.boco.customer.utils.Metadata;
import com.boco.customer.utils.RedisUtils;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.boco.customer.utils.DateUtils;
import com.boco.customer.utils.Metadata;

public class OReStXdrPsLteMmeMapper extends Mapper<Object, Text, Text, Text>  implements S1Mme  {

	private static Pattern pattern = Pattern.compile("\\|");
	public static Text keytext = new Text();
	public static Text valuetext = new Text();
	private int IMSI;
	private int MSISDN;
	private int TAC;
	private int ECI;
	private int PROCEDURE_STRAT_TIME;
	private int PROCEDURE_END_TIME;
	private int PROCEDURE_TYPE;
	private int FAILURE_CAUSE;
	private int PROCEDURE_STATUS;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		/*Metadata rc;
		// 读取相应的初始化配置文件
		rc = new Metadata("/config/OReStXdrPsLteCustrackMme.properties");
		IMSI = Integer.valueOf(rc.getValue("IMSI"));
		MSISDN = Integer.valueOf(rc.getValue("MSISDN"));
		TAC = Integer.valueOf(rc.getValue("TAC"));
		ECI = Integer.valueOf(rc.getValue("CELL_ID"));
		PROCEDURE_STRAT_TIME = Integer.valueOf(rc
				.getValue("PROCEDURE_STRAT_TIME"));
		PROCEDURE_END_TIME = Integer.valueOf(rc.getValue("PROCEDURE_END_TIME"));
		PROCEDURE_TYPE = Integer.valueOf(rc.getValue("PROCEDURE_TYPE"));
		FAILURE_CAUSE = Integer.valueOf(rc.getValue("FAILURE_CAUSE"));
		PROCEDURE_STATUS = Integer.valueOf(rc.getValue("PROCEDURE_STATUS"));
	*/
	}

	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] xdr = pattern.split(value.toString(), -1);
		//判断字段长度
		if (xdr.length < S1MME_LEN||!"451".endsWith(xdr[CITY])) {
			return;
		}
		int succ_sessions_cnt = 0;// 成功次数
		int att_cnt = 1;// 请求次数
		long business_time = 0;// 业务时长

		// 成功次数
		if (xdr[PROCEDURE_STATUS].equals("0")) {
			succ_sessions_cnt = 1;
		}
		// 连接次数
		// att_cnt = 1;
		// 时长
		business_time = Long.parseLong(xdr[PROCEDURE_END_TIME])
				- Long.parseLong(xdr[PROCEDURE_STRAT_TIME]);
		// 时间点
		String startTime = DateUtils.paserTime(xdr[PROCEDURE_STRAT_TIME],
				"yyyy-MM-dd HH:mm:ss");// 开始时间YYYY-MM-DD HH24:MI:SS
		String endTime = DateUtils.paserTime(xdr[PROCEDURE_END_TIME],
				"yyyy-MM-dd HH:mm:ss");// 结束时间YYYY-MM-DD HH24:MI:SS
		// 组织key
		StringBuffer k = new StringBuffer();
		k.append(xdr[IMSI]).append("|");
		k.append(xdr[MSISDN]).append("|");
		k.append(xdr[TAC]).append("|");
		k.append(xdr[ECI]).append("|");
		k.append(startTime).append("|");
		k.append(endTime).append("|");
		k.append(xdr[PROCEDURE_TYPE]).append("|");
		k.append("9").append("|");// 识别来源哪类数据，9标识mme
		k.append(xdr[FAILURE_CAUSE]);

		// 组织value
		StringBuffer v = new StringBuffer();
		v.append(succ_sessions_cnt).append("|");
		v.append(att_cnt).append("|");
		v.append(business_time).append("|");
		v.append(0);// 下行流量,mme无下行流量
		// 输出到reducer
		keytext.set(k.toString());
		valuetext.set(v.toString());
		context.write(keytext, valuetext);
	}

}
