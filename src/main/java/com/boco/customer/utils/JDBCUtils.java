package com.boco.customer.utils;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class JDBCUtils {

	private static  String PROPERTIES_PATH = "/config/jdbc.properties";
	private static  String EMPTY = "";
	//  MYSQL配置信息
	private static  String driver;
	private static  String url;
	private static  String user;
	private static  String password;
	
	// ORACLE.DW配置信息
	private static  String odriver;
	private static  String ourl;
	private static  String ouser;
	private static  String opassword;
	
	// ORACLE.ODS配置信息
	private static  String odsdriver;
	private static  String odsurl;
	private static  String odsuser;
	private static  String odspassword;
	
	private static Connection conn = null;
	
	static {
		Properties prop = ConfigUtils.getConfig(PROPERTIES_PATH);
		driver = prop.getProperty("jdbc.driver", EMPTY);
		url = prop.getProperty("jdbc.url", EMPTY);
		user = prop.getProperty("jdbc.username", EMPTY);
		password = prop.getProperty("jdbc.password", EMPTY);
		
		odriver = prop.getProperty("ojdbc.driver", EMPTY);
		ourl = prop.getProperty("ojdbc.url", EMPTY);
		ouser = prop.getProperty("ojdbc.username", EMPTY);
		opassword = prop.getProperty("ojdbc.password", EMPTY);
		
		odsdriver = prop.getProperty("odsjdbc.driver", EMPTY);
		odsurl = prop.getProperty("odsjdbc.url", EMPTY);
		odsuser = prop.getProperty("odsjdbc.username", EMPTY);
		odspassword = prop.getProperty("odsjdbc.password", EMPTY);
		prop = null;
	}

	private JDBCUtils() { }

	/**
	 * 返回mysql connection
	 * @return
	 */
	public static com.mysql.jdbc.Connection getMysqlConnection() {
		com.mysql.jdbc.Connection mysqlConn = null;
		try {
			Class.forName(driver);
			System.out.println(driver);
			System.out.println(url);
			mysqlConn = (com.mysql.jdbc.Connection)DriverManager.getConnection(url, user, password);
			mysqlConn.setDontTrackOpenResources(true);
			System.out.println("connection_mysql");
		} catch (Exception e) {
			System.out.println("jdbc connection error");
			e.printStackTrace();
		}
		return mysqlConn;
	}
	
	/**
	 * 返回orale connection
	 * @return
	 */
	public static Connection getOracleOdsConnection() {
		try {
			System.out.println(odsdriver);
			System.out.println(odsurl);
			System.out.println(odsuser +" "+ odspassword);
			Class.forName(odsdriver);
			conn = DriverManager.getConnection(odsurl, odsuser, odspassword);
		} catch (Exception e) {
			System.out.println("jdbc connection error");
			e.printStackTrace();
		}
		return conn;
	}
	
	/**
	 * 返回orale connection
	 * @return
	 */
	public static Connection getOracleDwConnection() {
		try {
			System.out.println(odriver);
			System.out.println(ourl);
			System.out.println(ouser+" "+opassword);
			Class.forName(odriver);
			conn = DriverManager.getConnection(ourl, ouser, opassword);
		} catch (Exception e) {
			System.out.println("jdbc connection error");
			e.printStackTrace();
		}
		return conn;
	}
	
	public static Connection getOracleConnection() {
		try {
			System.out.println(ourl + " " + ouser);
			Class.forName(odriver);
			conn = DriverManager.getConnection(ourl, ouser, opassword);
		} catch (Exception e) {
			System.out.println("jdbc connection error");
			e.printStackTrace();
		}
		return conn;
	}
	/**
	 * 关闭连接
	 * @param resultSet
	 * @param stmt
	 * @param conn
	 */
	public static void closeAll(ResultSet rs, PreparedStatement pstm) {
		if (null != rs) {
			try {
				rs.close();
			} catch (SQLException e) {
				System.out.println("ResultSet close error");
				e.printStackTrace();
			}
		}
		if (null != pstm) {
			try {
				pstm.close();
			} catch (SQLException e) {
				System.out.println("PreparedStatement close error");
				e.printStackTrace();
			}
		}
		if (null != conn) {
			try {
				conn.close();
			} catch (SQLException e) {
				System.out.println("Connection close error");
				e.printStackTrace();
			}
		}
	}
	
	public static void closeAll(PreparedStatement pstm) {
		if (null != pstm) {
			try {
				pstm.close();
			} catch (SQLException e) {
				System.out.println("PreparedStatement close error");
				e.printStackTrace();
			}
		}
		if (null != conn) {
			try {
				conn.close();
			} catch (SQLException e) {
				System.out.println("Connection close error");
				e.printStackTrace();
			}
		}
	}
	public static void main(String[] args) {
	    getMysqlConnection();
	}
}