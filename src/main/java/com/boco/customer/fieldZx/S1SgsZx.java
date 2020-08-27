package com.boco.customer.fieldZx;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class S1SgsZx {
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
	private int SGS_CAUSE=20;
	private int REJECT_CAUSE=21;
	private int CP_CAUSE=22;
	private int RP_CAUSE=23;
	private int USER_IPV4=24;
	private int USER_IPV6=25;
	private int MME_IP_ADD=26;
	private int MSC_SERVER_IP_ADD=29;
	private int MME_PORT=30;
	private int MSC_SERVER_PORT=31;
	private int SERVICE_INDICATOR=32;
	private int MME_NAME=33;
	private int TMSI=34;
	private int NEW_LAI=35;
	private int OLD_LAI=36;
	private int TAC=37;
	private int CELL_ID=38;
	private int CALLING_ID=54;
	private int VLR_NAME_LENGTH=55;
	private int VLR_NAME=56;
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
			sb.append(xdrdata[SGS_CAUSE]).append("|");
			sb.append(xdrdata[REJECT_CAUSE]).append("|");
			sb.append(xdrdata[CP_CAUSE]).append("|");
			sb.append(xdrdata[RP_CAUSE]).append("|");
			sb.append(xdrdata[USER_IPV4]).append("|");
			sb.append(xdrdata[USER_IPV6]).append("|");
			sb.append(xdrdata[MME_IP_ADD]).append("|");
			sb.append(xdrdata[MSC_SERVER_IP_ADD]).append("|");
			sb.append(xdrdata[MME_PORT]).append("|");
			sb.append(xdrdata[MSC_SERVER_PORT]).append("|");
			sb.append(xdrdata[SERVICE_INDICATOR]).append("|");
			sb.append(xdrdata[MME_NAME]).append("|");
			sb.append(xdrdata[TMSI]).append("|");
			sb.append(xdrdata[NEW_LAI]).append("|");
			sb.append(xdrdata[OLD_LAI]).append("|");
			sb.append(xdrdata[TAC]).append("|");
			sb.append(xdrdata[CELL_ID]).append("|");
			sb.append(xdrdata[CALLING_ID]).append("|");
			sb.append(xdrdata[VLR_NAME_LENGTH]).append("|");
			sb.append(xdrdata[VLR_NAME]).append("|");

			list_zx.add(sb.toString());
		}
		return list_zx;
	}
}
