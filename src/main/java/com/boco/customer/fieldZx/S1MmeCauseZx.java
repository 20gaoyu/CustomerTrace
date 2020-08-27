package com.boco.customer.fieldZx;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class S1MmeCauseZx {
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
	private int REQUEST_CAUSE=20;
	private int FAILURE_CAUSE=21;
	private int KEYWORD1=24;
	private int KEYWORD2=25;
	private int KEYWORD3=26;
	private int KEYWORD4=27;
	private int MME_UE_S1AP_ID=28;
	private int OLD_MME_GROUP_ID=29;
	private int OLD_MME_CODE=30;
	private int OLDM_TMSI=31;
	private int MMEGROUPID=32;
	private int MMECODE=33;
	private int M_TMSI=34;
	private int UE_IP_ADD_TYPE=35;
	private int USER_IP=36;
	private int MACHINE_IP_ADD_TYPE=37;
	private int MME_IP_ADD=38;
	private int ENB_IP_ADD=41;
	private int MME_PORT=42;
	private int ENB_PORT=43;
	private int TAC=44;
	private int CELL_ID=45;
	private int OTHER_TAC=61;
	private int OTHER_ECI=62;
	private int APN=63;
	private int EPS_BEARER_CNT=64;
	private int BEARER1ID=65;
	private int BEARER1TYPE=66;
	private int BEARER1QCI=67;
	private int BEARER1STATUS=68;
	private int BEARER1REQUESTCAUSE=69;
	private int BEARER1FAILURECAUSE=70;
	private int BEARER1ENBGTP_TEID=71;
	private int BEARER1SGWGTP_TEID=72;
	private static Pattern pattern = Pattern.compile("\\|");
	public List<String> ReturnListZx(List<String> list)
	{
		int len=list.size();
		List<String> list_zx =new ArrayList<String>();
		for(String lines:list)
		{
			
			String[] xdrdata = pattern.split(lines.toString(), -1);
			StringBuffer sb = new StringBuffer();
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
			sb.append(xdrdata[REQUEST_CAUSE]).append("|");
			sb.append(xdrdata[FAILURE_CAUSE]).append("|");
			sb.append(xdrdata[KEYWORD1]).append("|");
			sb.append(xdrdata[KEYWORD2]).append("|");
			sb.append(xdrdata[KEYWORD3]).append("|");
			sb.append(xdrdata[KEYWORD4]).append("|");
			sb.append(xdrdata[MME_UE_S1AP_ID]).append("|");
			sb.append(xdrdata[OLD_MME_GROUP_ID]).append("|");
			sb.append(xdrdata[OLD_MME_CODE]).append("|");
			sb.append(xdrdata[OLDM_TMSI]).append("|");
			sb.append(xdrdata[MMEGROUPID]).append("|");
			sb.append(xdrdata[MMECODE]).append("|");
			sb.append(xdrdata[M_TMSI]).append("|");
			sb.append(xdrdata[UE_IP_ADD_TYPE]).append("|");
			sb.append(xdrdata[USER_IP]).append("|");
			sb.append(xdrdata[MACHINE_IP_ADD_TYPE]).append("|");
			sb.append(xdrdata[MME_IP_ADD]).append("|");
			sb.append(xdrdata[ENB_IP_ADD]).append("|");
			sb.append(xdrdata[MME_PORT]).append("|");
			sb.append(xdrdata[ENB_PORT]).append("|");
			sb.append(xdrdata[TAC]).append("|");
			sb.append(xdrdata[CELL_ID]).append("|");
			sb.append(xdrdata[OTHER_TAC]).append("|");
			sb.append(xdrdata[OTHER_ECI]).append("|");
			sb.append(xdrdata[APN]).append("|");
			sb.append(xdrdata[EPS_BEARER_CNT]).append("|");
			sb.append(xdrdata[BEARER1ID]).append("|");
			sb.append(xdrdata[BEARER1TYPE]).append("|");
			sb.append(xdrdata[BEARER1QCI]).append("|");
			sb.append(xdrdata[BEARER1STATUS]).append("|");
			sb.append(xdrdata[BEARER1REQUESTCAUSE]).append("|");
			sb.append(xdrdata[BEARER1FAILURECAUSE]).append("|");
			sb.append(xdrdata[BEARER1ENBGTP_TEID]).append("|");
			sb.append(xdrdata[BEARER1SGWGTP_TEID]).append("|");
			sb.append(String.valueOf(len));

			list_zx.add(sb.toString());
		}
		return list_zx;
	}
}
