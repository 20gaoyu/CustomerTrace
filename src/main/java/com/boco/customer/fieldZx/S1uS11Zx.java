package com.boco.customer.fieldZx;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class S1uS11Zx {
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
	private int FAILURE_CAUSE=20;
	private int REQUEST_CAUSE=21;
	private int USER_IPV4=22;
	private int USER_IPV6=23;
	private int MME_ADDRESS=24;
	private int SGWOROLD_MME_ADDRESS=27;
	private int MME_PORT=30;
	private int SGWOROLD_MME_PORT=31;
	private int MME_CONTROL_TEID=32;
	private int OLDMMEORSGW_CONTROL_TEID=33;
	private int APN=34;
	private int EPS_BEARER_CNT=37;
	private int BEARER1ID=38;
	private int BEARER1TYPE=39;
	private int BEARER1QCI=40;
	private int BEARER1STATUS=41;
	private int BEARER1ENB_GTP_TEID=42;
	private int BEARER1SGW_GTP_TEID=43;
	private int BEARER2ID=44;
	private int BEARER2TYPE=45;
	private int BEARER2QCI=46;
	private int BEARER2STATUS=47;
	private int BEARER2ENB_GTP_TEID=48;
	private int BEARER2SGW_GTP_TEID=49;
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
			sb.append(xdrdata[FAILURE_CAUSE]).append("|");
			sb.append(xdrdata[REQUEST_CAUSE]).append("|");
			sb.append(xdrdata[USER_IPV4]).append("|");
			sb.append(xdrdata[USER_IPV6]).append("|");
			sb.append(xdrdata[MME_ADDRESS]).append("|");
			sb.append(xdrdata[SGWOROLD_MME_ADDRESS]).append("|");
			sb.append(xdrdata[MME_PORT]).append("|");
			sb.append(xdrdata[SGWOROLD_MME_PORT]).append("|");
			sb.append(xdrdata[MME_CONTROL_TEID]).append("|");
			sb.append(xdrdata[OLDMMEORSGW_CONTROL_TEID]).append("|");
			sb.append(xdrdata[APN]).append("|");
			sb.append(xdrdata[EPS_BEARER_CNT]).append("|");
			sb.append(xdrdata[BEARER1ID]).append("|");
			sb.append(xdrdata[BEARER1TYPE]).append("|");
			sb.append(xdrdata[BEARER1QCI]).append("|");
			sb.append(xdrdata[BEARER1STATUS]).append("|");
			sb.append(xdrdata[BEARER1ENB_GTP_TEID]).append("|");
			sb.append(xdrdata[BEARER1SGW_GTP_TEID]).append("|");
			sb.append(xdrdata[BEARER2ID]).append("|");
			sb.append(xdrdata[BEARER2TYPE]).append("|");
			sb.append(xdrdata[BEARER2QCI]).append("|");
			sb.append(xdrdata[BEARER2STATUS]).append("|");
			sb.append(xdrdata[BEARER2ENB_GTP_TEID]).append("|");
			sb.append(xdrdata[BEARER2SGW_GTP_TEID]).append("|");
			list_zx.add(sb.toString());
		}
		return list_zx;
	}
}

