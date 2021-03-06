package com.boco.customer.fieldZx;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class S1uGnrlZx {
	private int TIME_ID=3;
	private int LENGTH=4;
	private int CITY=5;
	private int INTERFACE=6;
	private int XDR_ID=7;
	private int RAT=8;
	private int IMSI=9;
	private int IMEI=10;
	private int MSISDN=11;
	private int MACHINE_IP_ADD_TYPE=17;
	private int SGWIPADD=18;
	private int ENODEBIPADD=25;
	private int SGW_PORT=26;
	private int ENODEB_PORT=27;
	private int ENB_GTP_TEID=28;
	private int SGW_GTP_TEID=29;
	private int TAC=30;
	private int ECI=31;
	private int APN=47;
	private int APP_TYPE_CODE=48;
	private int START_TIME=49;
	private int END_TIME=50;
	private int PROTOCOL_TYPE=61;
	private int APP_TYPE=62;
	private int APP_SUB_TYPE=64;
	private int APP_CONTENT=63;
	private int APP_STATUS=65;
	private int USER_IPV4=66;
	private int USER_IPV6=67;
	private int USER_PORT=68;
	private int L4_PROTOCAL=69;
	private int APP_SERVER_IP_IPV4=74;
	private int APP_SERVER_IP_IPV6=75;
	private int APP_SERVER_PORT=76;
	private int UL_DATA=77;
	private int DL_DATA=78;
	private int UL_IP_PACKET=79;
	private int DL_IP_PACKET=80;
	private int UL_TCP_DISCORDNUM=81;
	private int DL_TCP_DISCORDNUM=82;
	private int UL_TCP_RENUM=83;
	private int DL_TCP_RENUM=84;
	private int TCP_RESPONSE_DELAY=85;
	private int TCP_CONFIRM_DELAY=86;
	private int UL_IP_FRAG_PACKETS=87;
	private int DL_IP_FRAG_PACKETS=88;
	private int TCP_SUCC_REQUEST_DELAY=89;
	private int FIRST_RESPONSE_DELAY=90;
	private int WINDOW_SIZE=91;
	private int MSS_SIZE=92;
	private int TCP_ATT_CNT=93;
	private int TCP_CONN_STATUS=94;
	private int SESSION_MARK_END =95;
	
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
			sb.append(xdrdata[MACHINE_IP_ADD_TYPE]).append("|");
			sb.append(xdrdata[SGWIPADD]).append("|");
			sb.append(xdrdata[ENODEBIPADD]).append("|");
			sb.append(xdrdata[SGW_PORT]).append("|");
			sb.append(xdrdata[ENODEB_PORT]).append("|");
			sb.append(xdrdata[ENB_GTP_TEID]).append("|");
			sb.append(xdrdata[SGW_GTP_TEID]).append("|");
			sb.append(xdrdata[TAC]).append("|");
			sb.append(xdrdata[ECI]).append("|");
			sb.append(xdrdata[APN]).append("|");
			sb.append(xdrdata[APP_TYPE_CODE]).append("|");
			sb.append(xdrdata[START_TIME]).append("|");
			sb.append(xdrdata[END_TIME]).append("|");
			sb.append(xdrdata[PROTOCOL_TYPE]).append("|");
			sb.append(xdrdata[APP_TYPE]).append("|");
			sb.append(xdrdata[APP_SUB_TYPE]).append("|");
			sb.append(xdrdata[APP_CONTENT]).append("|");
			sb.append(xdrdata[APP_STATUS]).append("|");
			sb.append(xdrdata[USER_IPV4]).append("|");
			sb.append(xdrdata[USER_IPV6]).append("|");
			sb.append(xdrdata[USER_PORT]).append("|");
			sb.append(xdrdata[L4_PROTOCAL]).append("|");
			sb.append(xdrdata[APP_SERVER_IP_IPV4]).append("|");
			sb.append(xdrdata[APP_SERVER_IP_IPV6]).append("|");
			sb.append(xdrdata[APP_SERVER_PORT]).append("|");
			sb.append(xdrdata[UL_DATA]).append("|");
			sb.append(xdrdata[DL_DATA]).append("|");
			sb.append(xdrdata[UL_IP_PACKET]).append("|");
			sb.append(xdrdata[DL_IP_PACKET]).append("|");
			sb.append(xdrdata[UL_TCP_DISCORDNUM]).append("|");
			sb.append(xdrdata[DL_TCP_DISCORDNUM]).append("|");
			sb.append(xdrdata[UL_TCP_RENUM]).append("|");
			sb.append(xdrdata[DL_TCP_RENUM]).append("|");
			sb.append(xdrdata[TCP_RESPONSE_DELAY]).append("|");
			sb.append(xdrdata[TCP_CONFIRM_DELAY]).append("|");
			sb.append(xdrdata[UL_IP_FRAG_PACKETS]).append("|");
			sb.append(xdrdata[DL_IP_FRAG_PACKETS]).append("|");
			sb.append(xdrdata[TCP_SUCC_REQUEST_DELAY]).append("|");
			sb.append(xdrdata[FIRST_RESPONSE_DELAY]).append("|");
			sb.append(xdrdata[WINDOW_SIZE]).append("|");
			sb.append(xdrdata[MSS_SIZE]).append("|");
			sb.append(xdrdata[TCP_ATT_CNT]).append("|");
			sb.append(xdrdata[TCP_CONN_STATUS]).append("|");
			sb.append(xdrdata[SESSION_MARK_END ]).append("|");

			list_zx.add(sb.toString());
		}
		return list_zx;
	}
}
