package com.boco.customer.fieldZx;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class S6aZx {
	private int TIME_ID=3;
	private int LENGTH=4;
	private int CITY=5;
	private int INTERFACE=6;
	private int XDR_ID=7;
	private int RAT=8;
	private int IMSI=9;
	private int IMEI=10;
	private int MSISDN=11;
	private int PROCEDURE_TYPE=16;
	private int PROCEDURE_STRAT_TIME=17;
	private int PROCEDURE_END_TIME=18;
	private int PROCEDURE_STATUS=19;
	private int CAUSE=20;
	private int USER_IPV4=21;
	private int USER_IPV6=22;
	private int MME_ADDRESS=23;
	private int HSS_ADDRESS=26;
	private int MME_PORT=27;
	private int HSS_PORT=28;
	private int ORIGIN_REALM=29;
	private int DESTINATION_REALM=30;
	private int ORIGIN_HOST=31;
	private int DESTINATION_HOST=32;
	private int APPLICATION_ID=33;
	private int SUBSCRIBER_STATUS=34;
	private int ACCESS_RESTRICTION_DATA=35;
	
	private static Pattern pattern = Pattern.compile("\\|");
	public List<String> ReturnListZx(List<String> list)
	{
		
		List<String> list_zx =new ArrayList<String>();
		for(String lines:list)
		{
			
			String[] xdrdata = pattern.split(lines.toString(), -1);
			StringBuffer sb = new StringBuffer();
			  sb.append(xdrdata[TIME_ID]).append("|");
				sb.append(xdrdata[LENGTH]).append("|");
				sb.append(xdrdata[CITY]).append("|");
				sb.append(xdrdata[INTERFACE]).append("|");
				sb.append(xdrdata[XDR_ID]).append("|");
				sb.append(xdrdata[RAT]).append("|");
				sb.append(xdrdata[IMSI]).append("|");
				sb.append(xdrdata[IMEI]).append("|");
				sb.append(xdrdata[MSISDN]).append("|");
				sb.append(xdrdata[PROCEDURE_TYPE]).append("|");
				sb.append(xdrdata[PROCEDURE_STRAT_TIME]).append("|");
				sb.append(xdrdata[PROCEDURE_END_TIME]).append("|");
				sb.append(xdrdata[PROCEDURE_STATUS]).append("|");
				sb.append(xdrdata[CAUSE]).append("|");
				sb.append(xdrdata[USER_IPV4]).append("|");
				sb.append(xdrdata[USER_IPV6]).append("|");
				sb.append(xdrdata[MME_ADDRESS]).append("|");
				sb.append(xdrdata[HSS_ADDRESS]).append("|");
				sb.append(xdrdata[MME_PORT]).append("|");
				sb.append(xdrdata[HSS_PORT]).append("|");
				sb.append(xdrdata[ORIGIN_REALM]).append("|");
				sb.append(xdrdata[DESTINATION_REALM]).append("|");
				sb.append(xdrdata[ORIGIN_HOST]).append("|");
				sb.append(xdrdata[DESTINATION_HOST]).append("|");
				sb.append(xdrdata[APPLICATION_ID]).append("|");
				sb.append(xdrdata[SUBSCRIBER_STATUS]).append("|");
				sb.append(xdrdata[ACCESS_RESTRICTION_DATA]).append("|");

			list_zx.add(sb.toString());
		}
		return list_zx;
	}
}
