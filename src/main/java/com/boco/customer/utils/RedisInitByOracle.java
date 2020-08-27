package com.boco.customer.utils;

/**
 * @author 
 * 同步oracle维度数据更新redis数据库
 * 
 */
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

//import com.boco.DataPlatform.etl.tools.StringTools;

//java -cp .:../oozie/libserver/* com.boco.pms.utils.RedisInitByOracle  DIM_XDR_SUBAPP 
public class RedisInitByOracle {
	private static final String ALIAS = ".ORACEL_ALIAS";
	private static final String COLUMNS = ".COLUMNS";
	private static final String KEY_VALUE = ".KEY_VALUE";
	private static final String ORDERS = ".ORDERS";
	private static final String WHERE_CAUSE = ".WHERE_CAUSE";
	private static String HOST; // "10.0.7.239" redis host
	private static int PORT; // 6379 redis port
	private static JedisPoolConfig config = null; // Jedis客户端池配置
	private static JedisPool pool = null; // Jedis客户端池
	private static Jedis j = null;
	private static String PASSWORD	 = null;
	private static String PROVINCE;
	private static Properties prop;
	private static Pattern p = Pattern.compile(","); // 逗号分割
	private static String CONFIG_PATH = "/config/redis.properties";
	private static String[] INIT_TABLES;
	private static Connection conn = null;
	

	static {
		prop = ConfigUtils.getConfig(CONFIG_PATH);

		// filesPath = prop.getProperty("INIT_DATA_PATH");
		// filesTailName = prop.getProperty("INIT_FILE_TAIL_NAME");

		String initTables = prop.getProperty("REDIS_INIT_TABLES");
		INIT_TABLES = p.split(initTables);
		System.out.println("init tables is " + initTables);
		PASSWORD=prop.getProperty("PASSWORD");
		PROVINCE=prop.getProperty("PROVINCE");
		HOST = prop.getProperty("REDIS.HOST");
		PORT = Integer.parseInt(prop.getProperty("REDIS.PORT"));

		config = new JedisPoolConfig();
		config.setMaxActive(60000);
		config.setMaxIdle(1000);
		config.setMaxWait(10000);
		config.setTestOnBorrow(true);
		if("JILIN".equals(PROVINCE))
		pool = new JedisPool(config, HOST, PORT, 100000,PASSWORD);
		else
		pool = new JedisPool(config, HOST, PORT, 100000);
	}

	public static void initTable(String table) {
		try {
			conn = JDBCUtils.getOracleConnection();
			j = pool.getResource();
			Pipeline pipe = j.pipelined();
			String table_name = getRealName(table);
			String table_name_redis = table;
			String key_column = prop.getProperty(getKeyValue(table), "");
			String values_column = prop.getProperty(getColumns(table), "");
			String orders = prop.getProperty(getOrders(table), "");
			String where_cause = prop.getProperty(getWhereCause(table), "");
			pipe.hset("meta_table", table_name_redis,
					table_name + "|" + table_name_redis + "|" + key_column + "|" + values_column);
			pipe.sync();
			initTable(table_name, table_name_redis, key_column, values_column, orders, where_cause);
			pool.returnResource(j);

			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static void initTable(String table_name, String table_name_redis, String key_column, String values_column,
			String orders, String where_cause) {
		// 拼接key字段
		String[] key_cols = p.split(key_column);
		StringBuilder strBuilder_key = new StringBuilder();
		for (String column_name : key_cols) {
			strBuilder_key.append(column_name);
			strBuilder_key.append("||'|'||");
		}
		strBuilder_key.delete(strBuilder_key.length() - 7, strBuilder_key.length());
		String key_names = strBuilder_key.toString();

		// 拼接value字段
		String[] value_cols = p.split(values_column);
		StringBuilder strBuilder_values = new StringBuilder();
		for (String column_name : value_cols) {
			strBuilder_values.append(column_name);
			strBuilder_values.append("||'|'||");
		}
		strBuilder_values.delete(strBuilder_values.length() - 7, strBuilder_values.length());
		String value_names = strBuilder_values.toString();
		if (!(orders == null) && !orders.equals("")) {
			orders = "  order by " + orders;
		} else {
			orders = "";
		}
		if (!(where_cause == null) && !where_cause.equals("")) {
			where_cause = "  where " + where_cause;
		} else {
			where_cause = "";
		}
		String str_sql = "select " + key_names + "," + value_names + " from " + table_name + where_cause + orders;
		System.out.println(str_sql);
		str_sql = str_sql.replace("&", ",");
		try {
			System.out.println("开始初始化" + table_name);
			int count = 0;
			Pipeline pipe = j.pipelined();
			j.del(table_name_redis);
			PreparedStatement ps = conn.prepareStatement(str_sql);
			ResultSet rs = ps.executeQuery();
			String key;
			String values;
			Map<String, String> map_value = new HashMap<String, String>();
			while (rs.next()) {
				count++;
				key = rs.getString(1);
				values = rs.getString(2);
				// pipe.hset(table_name_redis, key, values);
				map_value.put(key, values);
				if (count % 10000 == 0) {
					pipe.hmset(table_name_redis, map_value);
					map_value.clear();
					System.out.println("插入" + count + "条  ");
				}
			}
			if (count % 10000 > 0) {
				pipe.hmset(table_name_redis, map_value);
			}
			rs.close();
			ps.close();
			pipe.sync();
			System.out.println("初始化" + table_name + "结束  ");
			System.out.println("共" + count + "条记录");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			System.out.println("初始化" + table_name + "失败");
		}
	}

	/*
	 * 初始化装载选定的表文件 从oracle读取配置信息
	 */
	@SuppressWarnings("unused")
	private static void initAllByOracle() {
		int count = 0; // 计数
		Pipeline pipe = j.pipelined();
		j.del("meta_table");
		try {
			PreparedStatement ps = conn.prepareStatement("select * from meta_redis");

			ResultSet rs = ps.executeQuery();

			String table_name;
			String key_column;
			String values_column;
			String table_name_redis;
			while (rs.next()) {
				table_name = rs.getString("table_name");
				table_name_redis = rs.getString("table_name_redis");
				key_column = rs.getString("key_column");
				values_column = rs.getString("values_column");
				pipe.hset("meta_table", table_name_redis,
						table_name + "|" + table_name_redis + "|" + key_column + "|" + values_column);

				count++;
			}
			rs.close();
			ps.close();
			pipe.sync();
		} catch (Exception e) {
			System.out.println("初始化失败");
		}
	}

	private static void initAll() {
		int count = 0; // 计数
		Pipeline pipe = j.pipelined();
		j.del("meta_table");
		try {

			String table_name;
			String key_column;
			String values_column;
			String table_name_redis;
			for (String table : INIT_TABLES) {
				// 获取oracle表名,字段相关信息
				System.out.println("init table is " + table);
				table_name = getRealName(table);

				table_name_redis = table;
				key_column = prop.getProperty(getKeyValue(table), "");
				values_column = prop.getProperty(getColumns(table), "");
				String orders = prop.getProperty(getOrders(table), "");
				String where_cause = prop.getProperty(getWhereCause(table), "");
				pipe.hset("meta_table", table_name_redis,
						table_name + "|" + table_name_redis + "|" + key_column + "|" + values_column);

				pipe.hset("meta_table", table_name_redis,
						table_name + "|" + table_name_redis + "|" + key_column + "|" + values_column);
				if (table_name.equals("CT_D_HOUSE_IPLIST")) {
					initzz(table_name, table_name_redis, key_column, values_column);
					continue;
				}
				// if (table_name.equals("CT_D_USER_SERDOAMIN_LIST")) {
				// initTable(table_name, table_name_redis, key_column,
				// values_column);
				// continue;
				// }
				initTable(table_name, table_name_redis, key_column, values_column, orders, where_cause);
				count++;
			}
			pipe.sync();
			System.out.println("共初始化" + count + "表");
		} catch (Exception e) {
			System.out.println("初始化失败");
		}
	}

	private static void initAll(String tab_name) {
		int count = 0; // 计数
		Pipeline pipe = j.pipelined();
		j.del("meta_table");
		try {

			String table_name;
			String key_column;
			String values_column;
			String table_name_redis;
			for (String table : INIT_TABLES) {
				// 获取oracle表名,字段相关信息
				if (table.equals(tab_name)) {

					table_name = getRealName(table);
					table_name_redis = table;
					key_column = prop.getProperty(getKeyValue(table), "");
					values_column = prop.getProperty(getColumns(table), "");
					String orders = prop.getProperty(getOrders(table), "");
					String where_cause = prop.getProperty(getWhereCause(table), "");
					pipe.hset("meta_table", table_name_redis,
							table_name + "|" + table_name_redis + "|" + key_column + "|" + values_column);

					pipe.hset("meta_table", table_name_redis,
							table_name + "|" + table_name_redis + "|" + key_column + "|" + values_column);
					if (table_name.equals("CT_D_HOUSE_IPLIST")) {
						initzz(table_name, table_name_redis, key_column, values_column);
						continue;
					}
					// if (table_name.equals("CT_D_USER_SERDOAMIN_LIST")) {
					// initTable(table_name, table_name_redis, key_column,
					// values_column);
					// continue;
					// }
					initTable(table_name, table_name_redis, key_column, values_column, orders, where_cause);

				}
				count++;
			}
			pipe.sync();
			System.out.println("共初始化" + count + "表");
		} catch (Exception e) {
			System.out.println("初始化失败");
		}
	}

	// 初始化数据
	public static void main(String[] args) {

		// initTable("P_FRSMMS_IDCINFO_MANAGE");

		try {
			conn = JDBCUtils.getOracleConnection();
			j = pool.getResource();
			if (args.length > 0 && args[0].equals("UPDATE")) {
				String tables = prop.getProperty("REDIS_UPDATE_TABLES");
				INIT_TABLES = p.split(tables);
				RedisInitByOracle.initAll();
				pool.returnResource(j);
				conn.close();
			} else if (args.length > 0) {
				RedisInitByOracle.initAll(args[0]);
				pool.returnResource(j);
				conn.close();
			} else {
				RedisInitByOracle.initAll();
				pool.returnResource(j);
				conn.close();
			}
		} catch (Exception e) {
			System.out.println("初始化失败");
			System.out.println(e.getMessage());
		}

		/*
		 * HOST = prop.getProperty("REDIS.HOST1"); pool = new JedisPool(config,
		 * HOST, PORT, 100000); try { conn = JDBCUtils.getOracleConnection(); j
		 * = pool.getResource(); if (args.length > 0 &&
		 * args[0].equals("UPDATE")) { String tables =
		 * prop.getProperty("REDIS_UPDATE_TABLES"); INIT_TABLES =
		 * p.split(tables); RedisInitByOracle.initAll(); pool.returnResource(j);
		 * conn.close(); } else if (args.length > 0) {
		 * RedisInitByOracle.initAll(args[0]); pool.returnResource(j);
		 * conn.close(); } else { RedisInitByOracle.initAll();
		 * pool.returnResource(j); conn.close(); }
		 * 
		 * } catch (Exception e) { System.out.println("初始化失败");
		 * System.out.println(e.getMessage()); }
		 */
	}

	// 返回tableName.ALIAS
	private static String getAlias(String tableName) {
		return StringTools.append(tableName, ALIAS);
	}

	// 返回tableName.COLUMNS
	private static String getColumns(String tableName) {
		return StringTools.append(tableName, COLUMNS);
	}

	// 返回tableName.ORDER
	private static String getOrders(String tableName) {
		return StringTools.append(tableName, ORDERS);
	}

	// 返回tableName.KEY_VALUE
	private static String getKeyValue(String tableName) {
		return StringTools.append(tableName, KEY_VALUE);
	}

	// 返回tableName.WHERE_CAUSE
	private static String getWhereCause(String tableName) {
		return StringTools.append(tableName, WHERE_CAUSE);
	}

	// 根据tableName获取数据库真实表的name
	private static String getRealName(String tableName) {
		return prop.getProperty(getAlias(tableName), "");
	}

	private static void initzz(String table_name, String table_name_redis, String key_column, String values_column) {
		// 拼接key字段
		String[] key_cols = p.split(key_column);
		StringBuilder strBuilder_key = new StringBuilder();
		for (String column_name : key_cols) {
			strBuilder_key.append(column_name);
			strBuilder_key.append("||'|'||");
		}
		strBuilder_key.delete(strBuilder_key.length() - 7, strBuilder_key.length());
		String key_names = strBuilder_key.toString();

		// 拼接value字段
		String[] value_cols = p.split(values_column);
		StringBuilder strBuilder_values = new StringBuilder();
		for (String column_name : value_cols) {
			strBuilder_values.append(column_name);
			strBuilder_values.append("||'|'||");
		}
		strBuilder_values.delete(strBuilder_values.length() - 7, strBuilder_values.length());
		String value_names = strBuilder_values.toString();
		String str_sql = "select " + key_names + "," + value_names + " from " + table_name;

		try {
			System.out.println("开始初始化" + table_name);
			int count = 0;
			Pipeline pipe = j.pipelined();
			j.del(table_name_redis);
			PreparedStatement ps = conn.prepareStatement(str_sql);
			ResultSet rs = ps.executeQuery();
			String key;
			String values;
			Map<String, String> map_value = new HashMap<String, String>();
			while (rs.next()) {
				count++;
				key = rs.getString(1);
				// System.out.println("key"+key);
				values = rs.getString(2);
				// System.out.println("values"+values);
				@SuppressWarnings("unused")
				String[] str_value = values.split("\\|");
				/*
				 * //SortIp sortIp = new SortIp(); //sortIp.sort(values); //
				 * pipe.hset(table_name_redis, key, values); values =
				 * str_value[0] + "|" + sortIp.value_list + "|" +
				 * sortIp.fir_iplist + "|" + sortIp.sec_iplist;
				 */
				map_value.put(key, values);
				if (count % 10000 == 0) {
					pipe.hmset(table_name_redis, map_value);
					map_value.clear();
					System.out.println("插入" + count + "条  ");
				}
			}
			if (count % 10000 > 0) {
				pipe.hmset(table_name_redis, map_value);
			}
			rs.close();
			ps.close();
			pipe.sync();
			System.out.println("初始化" + table_name + "结束  ");
			System.out.println("共" + count + "条记录");
		} catch (Exception e) {
			// System.out.println(e.getMessage());
			System.out.println(e.toString());
			System.out.println("初始化" + table_name + "失败");
		}
	}

}
