package com.boco.customer.fieldZx;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class S1uHttpCauseZx {
	private int CITY=5;
	private int INTERFACE=6;
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
	private int APN=57;
	private int APP_TYPE_CODE=58;
	private int START_TIME=59;
	private int END_TIME=60;
	private int PROTOCOL_TYPE=61;
	private int APP_TYPE=62;
	private int APP_SUB_TYPE=63;
	private int APP_STATUS=64;
	private int UL_DATA=76;
	private int DL_DATA=77;
	private int TCP_RESPONSE_DELAY=84;
	private int TCP_CONFIRM_DELAY=85;
	private int TCP_SUCC_REQUEST_DELAY=88;
	private int FIRST_RESPONSE_DELAY=89;
	private int TCP_ATT_CNT=92;
	private int TCP_CONN_STATUS=93;
	private int SESSION_MARK_END =94;
	private int TRANSACTION_TYPE=96;
	private int HTTP_WAP_STATUS=97;
	private int FIRST_HTTP_RES_DELAY=100;
	private int HTTP_RESPONSE_LAST=101;
	private int ACK_PACKET=102;
	private int BUSSI_DELAY=118;
	private static Pattern pattern = Pattern.compile("\\|");
	public List<String> ReturnListZx(List<String> list)
	{
		int len=list.size();
		List<String> list_zx =new ArrayList<String>();
		for(String lines:list)
		{
			
			String[] xdrdata = pattern.split(lines.toString(), -1);
			StringBuffer sb = new StringBuffer();
			sb.append(xdrdata[CITY]).append("|");
			sb.append(xdrdata[INTERFACE]).append("|");
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
			sb.append(xdrdata[APP_STATUS]).append("|");
			sb.append(xdrdata[UL_DATA]).append("|");
			sb.append(xdrdata[DL_DATA]).append("|");
			sb.append(xdrdata[TCP_RESPONSE_DELAY]).append("|");
			sb.append(xdrdata[TCP_CONFIRM_DELAY]).append("|");
			sb.append(xdrdata[TCP_SUCC_REQUEST_DELAY]).append("|");
			sb.append(xdrdata[FIRST_RESPONSE_DELAY]).append("|");
			sb.append(xdrdata[TCP_ATT_CNT]).append("|");
			sb.append(xdrdata[TCP_CONN_STATUS]).append("|");
			sb.append(xdrdata[SESSION_MARK_END ]).append("|");
			sb.append(xdrdata[TRANSACTION_TYPE]).append("|");
			sb.append(xdrdata[HTTP_WAP_STATUS]).append("|");
			sb.append(xdrdata[FIRST_HTTP_RES_DELAY]).append("|");
			sb.append(xdrdata[HTTP_RESPONSE_LAST]).append("|");
			sb.append(xdrdata[ACK_PACKET]).append("|");
			sb.append(xdrdata[BUSSI_DELAY]).append("|");
			sb.append(String.valueOf(len));
			list_zx.add(sb.toString());
		}
		return list_zx;
	}
}
