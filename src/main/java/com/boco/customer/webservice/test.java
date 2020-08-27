package com.boco.customer.webservice;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

//import oracle.sql.DATE;

import org.apache.commons.lang.StringUtils;

import com.boco.customer.utils.MD5RowKeyGenerator;

public class test {
	private static Pattern pattern = Pattern.compile("\\|");
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*byte[] f;
		String str1="";
		f=getScanStartOrStop("20160615095549","18220000000",(byte) 9);
		StringBuffer sb=new StringBuffer();
		 for (byte element: f )
		 {
		 sb.append(String.valueOf(element));
		 }
		 String table_name="O_RE_ST_XDR_PS_S1U_RTSP_DETAIL";
		table_name=table_name.replace("DETAIL", "INDEX");
		System.out.println(table_name);*/
//		List<String> test = new ArrayList<String>();
//		String ttString="20160724|11|58|2016-07-24 11:58:34.034|2266|29|11|FEEC4AEE008CF0C14F00000000010000|6|460022917725389|8363739303530323|13689270000|83637393||||3|1|100.83.123.49|9999|未知|9999|未知|9999|未知|100.83.8.52|2152|2152|29298|159927881|37287|151013123|33|未知|9999|未知|||||29||未知|9999|9999|9999|未知||4||4004|||||1|未知|ffffffffffffffffffffffffffffffff|103|1469332713756|1469332714856|2|4|1|4004|0|10.105.206.93||60842|0|2362253863|未知|330000|未知|140.205.34.39||80|1101|718|5|5|0|0|0|0|0|0|47|174|234|52|15858|1400|1|0|1|3|6|200|未知(200)|未知|52|52|605|apiinit.amap.com|apiinit.amap.com/v3/log/init|ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff|AMAP Location SDK Android 1.3.1|application/x-www-form-urlencoded|ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff|ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff|86|0|255|255|ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff|ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff|3|0|1100|0|0|2016-07-24 18:21:51";
//		test.add(ttString);
//		List<String> failBusiList = new ArrayList<String>();
//		//failBusiList = cellCount(test, 65, 11, 33);
//		String tString="20160724".substring(4,8);
//		System.out.println(tString);
//		String dateString = "2016-08-06 00:00:05";  
//		try  
//		{  
//		    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
//		    Date date = sdf.parse(dateString); 
//		    System.out.println(date.getTime());
//		}  
//		catch (ParseException e)  
//		{  
//		    System.out.println(e.getMessage());  
//		}  
//		byte[] byBuffer = new byte[20];
//		String strRead = new String(getScanTimeHb("2016-08-06 00:00:05","15210568447"));
//		strRead = String.copyValueOf(strRead.toCharArray(), 0, 24); 
		for(String ss:DnsUserFcauseGroup("13521000000","","","1,2,6"))
		{
			System.out.println(ss);	
		}
		
		
		
	}
	public static byte[] getScanTimeHb(String time, String msisdn) {
		int keyLength = 24;
		int misLen = 11;
		int dateLen = 10;
		int md5Len = 3;
		byte[] k = new byte[keyLength];
		byte[] md5hash= new byte[3];
		byte[] datehash = new byte[10];
		byte[] msisdnhash = new byte[11];
		Date date=null;
		String dateString = time;
		//String dateString = "2016-08-06 00:00:05";  
		try  
		{  
		    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
		    date = sdf.parse(dateString); 
		    System.out.println("时间"+date.getTime());
		}  
		catch (ParseException e)  
		{  
			System.out.println(e.getMessage());  
		} 
		String md5_str=new MD5RowKeyGenerator().generatePrefix("15210568447").toString();
		
		md5hash=md5_str.getBytes();
		msisdnhash=msisdn.getBytes();
		datehash=Long.toString(date.getTime()).substring(0,10).getBytes();
		System.arraycopy(md5hash, 0, k, 0, md5Len);
		System.arraycopy(msisdnhash, 0, k, md5Len, misLen);
		System.arraycopy(datehash, 0, k, md5Len+misLen, dateLen);
		return k;
	}

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
	
	public static List<String> cellCount(List<String> list, int statusIndex,
			int msisdnIndex, int cellIndex) {
		List<String> lt = new ArrayList<String>();
		HashMap<String, int[]> hm = new HashMap<String, int[]>();
		// key：号码|小区 value:失败次数，成功次数，请求次数
		for (int i = 0; i < list.size(); i++) {
			String[] s = pattern.split(list.get(i), -1);
			String key = s[msisdnIndex] + "|" + s[cellIndex];
			System.out.println("key:"+key);
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
			System.out.println(key + "|" + val[0] + "|" + val[1] + "|" + val[2]);
			// 失败次数大于0
			if (val[0] > 0) {
				lt.add(key + "|" + val[0] + "|" + val[1] + "|" + val[2]);
				System.out.println(key + "|" + val[0] + "|" + val[1] + "|" + val[2]);
			}
		}
		return lt;
	}
	public static List<String> DnsUserFcauseGroup(String msisdn, String startTime, String endTime,String cause) {
		// 查询详单结果
		List<String> list = new ArrayList<String>();
		list.add("20170507|07|25|2017-05-07 07:25:27.027|2|451|11|15060435fa1a4a00|6|460001290243671|359176071343722|13521000000|35917607||||3|1|100.72.253.8|9999||9999||9999||100.72.145.196|2152|2152|00004eb1|aba01bb3|CMNET|17782|6A6A2B6|3639459993297249719||9120682511255764878||1981101301||||-257096336|||126.671873|45.741776|8||101|1494113127970|1494113127971|18000501|18||501|DNS|||||null|4|18|501||10.123.63.8||62205|1|218.203.61.216||53|72|244|1|1|0|0|0|0|0|0|0|0||1|0|0|||1|140.205.156.139||1|Format error||48|2|2|4|2017-05-07 11:12:32");
		list.add("20170507|07|25|2017-05-07 07:25:27.027|2|451|11|15060435fa1a4a00|6|460001290243671|359176071343722|13521000000|35917607||||3|1|100.72.253.8|9999||9999||9999||100.72.145.196|2152|2152|00004eb1|aba01bb3|CMNET|17782|6A6A2B6|3639459993297249719||9120682511255764878||1981101301||||-257096336|||126.671873|45.741776|8||101|1494113127970|1494113127971|18000501|18||501|DNS|||||null|4|18|501||10.123.63.8||62205|1|218.203.61.216||53|72|244|1|1|0|0|0|0|0|0|0|0||1|0|0|||1|140.205.156.139||2|Format error||48|2|2|4|2017-05-07 11:12:32");
		list.add("20170507|07|25|2017-05-07 07:25:27.027|2|451|11|15060435fa1a4a00|6|460001290243671|359176071343722|13521000000|35917607||||3|1|100.72.253.8|9999||9999||9999||100.72.145.196|2152|2152|00004eb1|aba01bb3|CMNET|17782|6A6A2B6|3639459993297249719||9120682511255764878||1981101301||||-257096336|||126.671873|45.741776|8||101|1494113127970|1494113127971|18000501|18||501|DNS|||||null|4|18|501||10.123.63.8||62205|1|218.203.61.216||53|72|244|1|1|0|0|0|0|0|0|0|0||1|0|0|||1|140.205.156.139||8|Format error||48|2|2|4|2017-05-07 11:12:32");
		System.out.println("list的大小："+list.size());

		if (list.size() == 0) {
			list.add(0, "0");
			System.out.println("list的大小为0："+list.size());
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
					System.out.println("存在相同原因码不能跳过，原因码DNS_RESP_CODE："+DNS_RESP_CODE);
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
				DNS_RESP_DELAY=Long.parseLong(line[50].trim())- Long.parseLong(line[49].trim()) ;
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			filter.add(DNS_RESP_FAIL_CNT);
			filter.add(DNS_RESP_CNT);
			filter.add(DNS_RESP_DELAY);
			if(tMap.containsKey(keyGroupBy)){
				tMap.get(keyGroupBy).set(0,(tMap.get(keyGroupBy).get(0)+DNS_RESP_FAIL_CNT));
				tMap.get(keyGroupBy).set(1,(tMap.get(keyGroupBy).get(1)+DNS_RESP_CNT));
				tMap.get(keyGroupBy).set(2,(tMap.get(keyGroupBy).get(2)+DNS_RESP_DELAY));
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
		System.out.println("汇总后的记录："+sb.toString());
		filter_return.add(sb.toString());
		
		}
		
		return filter_return;
	}
	protected static String addDefault(String currentValue, String defaultValue) {
		return StringUtils.isBlank(currentValue) ? defaultValue : currentValue;
	}
}
