/**
 * 数据库工具
 * 
 * @class JdbcUtil
 * @author 0.5
 */

package com.quickutil.platform;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.quickutil.platform.def.ResultSetDef;

public class JdbcUtil {

	private static Map<String, ComboPooledDataSource> dataSourceMap = new HashMap<String, ComboPooledDataSource>();

	/**
	 * 增加datasource，使用默认c3p0配置
	 * 
	 * @param jdbc-jdbc配置
	 */
	public static void addDataSource(Properties jdbc) {
		addDataSource(jdbc, null);
	}

	/**
	 * 增加datasource，指定c3p0配置
	 * 
	 * @param jdbc-jdbc配置
	 * @param c3p0-c3p0配置
	 */
	public static void addDataSource(Properties jdbc, Properties c3p0) {
		Enumeration<?> keys = jdbc.propertyNames();
		List<String> keyList = new ArrayList<String>();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			key = key.split("\\.")[0];
			if (!keyList.contains(key)) {
				keyList.add(key);
			}
		}
		for (String key : keyList) {
			try {
				String jdbcUrl = jdbc.getProperty(key + ".url");
				String user = jdbc.getProperty(key + ".username");
				String password = jdbc.getProperty(key + ".password");
				int initconnum = Integer.parseInt(jdbc.getProperty(key + ".initconnum"));
				int minconnum = Integer.parseInt(jdbc.getProperty(key + ".minconnum"));
				int maxconnum = Integer.parseInt(jdbc.getProperty(key + ".maxconnum"));
				ComboPooledDataSource datasource = getDataSource(jdbcUrl, user, password, initconnum, minconnum, maxconnum, c3p0);
				dataSourceMap.put(key, datasource);
				// System.out.println(JsonUtil.toJson(datasource.getConnection().getMetaData().getDatabaseProductName()));
				// System.out.println(JsonUtil.toJson(datasource.getConnection().getMetaData().getDatabaseMajorVersion()));
			} catch (Exception e) {
				LogUtil.error(e, "jdbc配置参数错误");
			}
		}
	}

	/**
	 * 增加datasource，使用默认c3p0配置
	 * 
	 * @param dbName-数据库名称
	 * @param url-jdbc的url
	 * @param username-用户名
	 * @param password-密码
	 * @param initconnum-初始化连接数
	 * @param minconnum-最小连接数
	 * @param maxconnum-最大连接数
	 */
	public static void addDataSource(String dbName, String url, String username, String password, int initconnum, int minconnum, int maxconnum) {
		dataSourceMap.put(dbName, getDataSource(url, username, password, initconnum, minconnum, maxconnum, null));
	}

	/**
	 * 增加datasource，使用指定c3p0配置
	 * 
	 * @param dbName-数据库名称
	 * @param url-jdbc的url
	 * @param username-用户名
	 * @param password-密码
	 * @param initconnum-初始化连接数
	 * @param minconnum-最小连接数
	 * @param maxconnum-最大连接数
	 * @param c3p0-c3p0配置
	 */
	public static void addDataSource(String dbName, String url, String username, String password, int initconnum, int minconnum, int maxconnum, Properties c3p0) {
		dataSourceMap.put(dbName, getDataSource(url, username, password, initconnum, minconnum, maxconnum, c3p0));
	}

	/**
	 * 获取已生成的datasource
	 * 
	 * @param dbName-数据库名称
	 * @return
	 */
	public static ComboPooledDataSource getDataSource(String dbName) {
		return dataSourceMap.get(dbName);
	}

	private static ComboPooledDataSource getDataSource(String jdbcUrl, String username, String password, int initconnum, int minconnum, int maxconnum, Properties c3p0) {
		if (jdbcUrl == null || username == null || password == null) {
			return null;
		}
		ComboPooledDataSource datasource = new ComboPooledDataSource();
		try {
			datasource.setDriverClass("com.mysql.jdbc.Driver");
			datasource.setJdbcUrl(jdbcUrl);
			datasource.setUser(username);
			datasource.setPassword(password);
			datasource.setInitialPoolSize(initconnum);
			datasource.setMinPoolSize(minconnum);
			datasource.setMaxPoolSize(maxconnum);
			// properties
			if (c3p0 == null)
				return datasource;
			if (c3p0.getProperty("AcquireIncrement") != null)
				datasource.setAcquireIncrement(Integer.parseInt(c3p0.getProperty("AcquireIncrement")));
			if (c3p0.getProperty("AcquireRetryAttempts") != null)
				datasource.setAcquireRetryAttempts(Integer.parseInt(c3p0.getProperty("AcquireRetryAttempts")));
			if (c3p0.getProperty("AcquireRetryDelay") != null)
				datasource.setAcquireRetryDelay(Integer.parseInt(c3p0.getProperty("AcquireRetryDelay")));
			if (c3p0.getProperty("AutoCommitOnClose") != null)
				datasource.setAutoCommitOnClose(Boolean.parseBoolean(c3p0.getProperty("AutoCommitOnClose")));
			if (c3p0.getProperty("AutomaticTestTable") != null)
				datasource.setAutomaticTestTable(c3p0.getProperty("AutomaticTestTable"));
			if (c3p0.getProperty("BreakAfterAcquireFailure") != null)
				datasource.setBreakAfterAcquireFailure(Boolean.parseBoolean(c3p0.getProperty("BreakAfterAcquireFailure")));
			if (c3p0.getProperty("CheckoutTimeout") != null)
				datasource.setCheckoutTimeout(Integer.parseInt(c3p0.getProperty("CheckoutTimeout")));
			if (c3p0.getProperty("ConnectionCustomizerClassName") != null)
				datasource.setConnectionCustomizerClassName(c3p0.getProperty("ConnectionCustomizerClassName"));
			if (c3p0.getProperty("ConnectionTesterClassName") != null)
				datasource.setConnectionTesterClassName(c3p0.getProperty("ConnectionTesterClassName"));
			if (c3p0.getProperty("ContextClassLoaderSource") != null)
				datasource.setContextClassLoaderSource(c3p0.getProperty("ContextClassLoaderSource"));
			if (c3p0.getProperty("DataSourceName") != null)
				datasource.setDataSourceName(c3p0.getProperty("DataSourceName"));
			if (c3p0.getProperty("DebugUnreturnedConnectionStackTraces") != null)
				datasource.setDebugUnreturnedConnectionStackTraces(Boolean.parseBoolean(c3p0.getProperty("DebugUnreturnedConnectionStackTraces")));
			if (c3p0.getProperty("Description") != null)
				datasource.setDescription(c3p0.getProperty("Description"));
			if (c3p0.getProperty("DriverClass") != null)
				datasource.setDriverClass(c3p0.getProperty("DriverClass"));
			if (c3p0.getProperty("FactoryClassLocation") != null)
				datasource.setFactoryClassLocation(c3p0.getProperty("FactoryClassLocation"));
			if (c3p0.getProperty("ForceIgnoreUnresolvedTransactions") != null)
				datasource.setForceIgnoreUnresolvedTransactions(Boolean.parseBoolean(c3p0.getProperty("ForceIgnoreUnresolvedTransactions")));
			if (c3p0.getProperty("ForceSynchronousCheckins") != null)
				datasource.setForceSynchronousCheckins(Boolean.parseBoolean(c3p0.getProperty("ForceSynchronousCheckins")));
			if (c3p0.getProperty("ForceUseNamedDriverClass") != null)
				datasource.setForceUseNamedDriverClass(Boolean.parseBoolean(c3p0.getProperty("ForceUseNamedDriverClass")));
			if (c3p0.getProperty("IdentityToken") != null)
				datasource.setIdentityToken(c3p0.getProperty("IdentityToken"));
			if (c3p0.getProperty("IdleConnectionTestPeriod") != null)
				datasource.setIdleConnectionTestPeriod(Integer.parseInt(c3p0.getProperty("IdleConnectionTestPeriod")));
			if (c3p0.getProperty("LoginTimeout") != null)
				datasource.setLoginTimeout(Integer.parseInt(c3p0.getProperty("LoginTimeout")));
			if (c3p0.getProperty("MaxAdministrativeTaskTime") != null)
				datasource.setMaxAdministrativeTaskTime(Integer.parseInt(c3p0.getProperty("MaxAdministrativeTaskTime")));
			if (c3p0.getProperty("MaxConnectionAge") != null)
				datasource.setMaxConnectionAge(Integer.parseInt(c3p0.getProperty("MaxConnectionAge")));
			if (c3p0.getProperty("MaxIdleTime") != null)
				datasource.setMaxIdleTime(Integer.parseInt(c3p0.getProperty("MaxIdleTime")));
			if (c3p0.getProperty("MaxStatements") != null)
				datasource.setMaxStatements(Integer.parseInt(c3p0.getProperty("MaxStatements")));
			if (c3p0.getProperty("MaxStatementsPerConnection") != null)
				datasource.setMaxStatementsPerConnection(Integer.parseInt(c3p0.getProperty("MaxStatementsPerConnection")));
			if (c3p0.getProperty("NumHelperThreads") != null)
				datasource.setNumHelperThreads(Integer.parseInt(c3p0.getProperty("NumHelperThreads")));
			if (c3p0.getProperty("OverrideDefaultPassword") != null)
				datasource.setOverrideDefaultPassword(c3p0.getProperty("OverrideDefaultPassword"));
			if (c3p0.getProperty("OverrideDefaultUser") != null)
				datasource.setOverrideDefaultUser(c3p0.getProperty("OverrideDefaultUser"));
			if (c3p0.getProperty("PreferredTestQuery") != null)
				datasource.setPreferredTestQuery(c3p0.getProperty("PreferredTestQuery"));
			if (c3p0.getProperty("PrivilegeSpawnedThreads") != null)
				datasource.setPrivilegeSpawnedThreads(Boolean.parseBoolean(c3p0.getProperty("PrivilegeSpawnedThreads")));
			if (c3p0.getProperty("PropertyCycle") != null)
				datasource.setPropertyCycle(Integer.parseInt(c3p0.getProperty("PropertyCycle")));
			if (c3p0.getProperty("StatementCacheNumDeferredCloseThreads") != null)
				datasource.setStatementCacheNumDeferredCloseThreads(Integer.parseInt(c3p0.getProperty("StatementCacheNumDeferredCloseThreads")));
			if (c3p0.getProperty("TestConnectionOnCheckin") != null)
				datasource.setTestConnectionOnCheckin(Boolean.parseBoolean(c3p0.getProperty("TestConnectionOnCheckin")));
			if (c3p0.getProperty("TestConnectionOnCheckout") != null)
				datasource.setTestConnectionOnCheckout(Boolean.parseBoolean(c3p0.getProperty("TestConnectionOnCheckout")));
			if (c3p0.getProperty("UnreturnedConnectionTimeout") != null)
				datasource.setUnreturnedConnectionTimeout(Integer.parseInt(c3p0.getProperty("UnreturnedConnectionTimeout")));
			if (c3p0.getProperty("UserOverridesAsString") != null)
				datasource.setUserOverridesAsString(c3p0.getProperty("UserOverridesAsString"));
			if (c3p0.getProperty("UsesTraditionalReflectiveProxies") != null)
				datasource.setUsesTraditionalReflectiveProxies(Boolean.parseBoolean(c3p0.getProperty("UsesTraditionalReflectiveProxies")));
		} catch (Exception e) {
			LogUtil.error(e, "c3p0配置错误");
		}
		return datasource;
	}

	/**
	 * 用于输入格式化，避免sql注入
	 * 
	 * @param sql-SQL语句
	 * @param escape-SQL转义符，例如MySQL使用\\\\、PG使用'
	 * @param params-参数，数组只支持string[]，格式会被转义成'a','b','c','d'
	 * @return
	 */
	public static String format(String sql, String escape, Object... params) {
		for (int i = 0; i < params.length; i++) {
			if (params[i] instanceof String[]) {
				StringBuilder sb = new StringBuilder();
				String[] array = (String[]) params[i];
				for (String sn : array) {
					sn = sn.replaceAll("'", escape + "'");
					sb.append("'" + sn + "',");
				}
				params[i] = sb.substring(0, sb.length() - 1);
			} else {
				params[i] = params[i].toString().replaceAll("'", escape + "'");
			}
		}
		return String.format(sql, params);
	}

	/**
	 * 将数组拼接为in查询所需字符串，并避免sql注入
	 * 
	 * @param array-内容数组
	 * @param escape-SQL转义符，例如MySQL使用\\\\、PG使用'
	 * @return
	 */
	public static String arrayToIn(String[] array, String escape) {
		if (array.length == 0)
			return null;
		StringBuilder sb = new StringBuilder();
		for (String sn : array) {
			sn = sn.replaceAll("'", escape + "'");
			sb.append("'" + sn + "',");
		}
		return sb.substring(0, sb.length() - 1);
	}

	private static String isTableExistSql = "select table_name FROM information_schema.TABLES WHERE table_name = '%s'";

	/**
	 * 检测表是否存在
	 * 
	 * @param dbName-数据库名称
	 * @param tableName-表名
	 * @return
	 */
	public static boolean isTableExist(String dbName, String tableName) {
		List<Object> list = getListString(dbName, String.format(isTableExistSql, tableName));
		if (list.size() > 0)
			return true;
		else
			return false;
	}

	private static String dropTableSql = "drop table `%s`";

	/**
	 * 删除表
	 * 
	 * @param dbName-数据库名称
	 * @param tableName-表名
	 * @return
	 */
	public static boolean dropTable(String dbName, String tableName) {
		return execute(dbName, String.format(dropTableSql, tableName));
	}

	private static String getTableListSql = "select TABLE_NAME from information_schema.tables where TABLE_SCHEMA='%s' and ENGINE='InnoDB'";

	/**
	 * 获取表名列表
	 * 
	 * @param dbName-数据库名称
	 * @param schema-schema名称
	 * @return
	 */
	public static List<String> getTableList(String dbName, String schema) {
		List<Object> list = JdbcUtil.getListString(dbName, String.format(getTableListSql, schema));
		List<String> tableList = new ArrayList<String>();
		for (Object o : list)
			tableList.add((String) o);
		return tableList;
	}

	private static String getCreateTableSql = "show create table %s";

	/**
	 * 获取创建表语句
	 * 
	 * @param dbName-数据库名称
	 * @param tableName-表名
	 * @return
	 */
	public static String getCreateTable(String dbName, String tableName) {
		List<Map<String, Object>> list = JdbcUtil.getListMap(dbName, String.format(getCreateTableSql, tableName));
		if (list.size() > 0)
			return (String) list.get(0).get("Create Table");
		return null;
	}

	private static String getTableColumnsSql = "select COLUMN_NAME from information_schema.columns where table_name='%s'";

	/**
	 * 获取表字段名
	 * 
	 * @param dbName-数据库名称
	 * @param tableName-表名
	 * @return
	 */
	public static List<String> getTableColumns(String dbName, String tableName) {
		List<Object> list = JdbcUtil.getListString(dbName, String.format(getTableColumnsSql, tableName));
		List<String> columnList = new ArrayList<String>();
		for (Object o : list)
			columnList.add((String) o);
		return columnList;
	}

	/**
	 * 获取单列数据
	 * 
	 * @param dbName-数据库名称
	 * @param sql-语句
	 * @return
	 */
	public static List<Object> getListString(String dbName, String sql) {
		List<Object> list = new ArrayList<Object>();
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			connection = dataSourceMap.get(dbName).getConnection();
			ps = connection.prepareStatement(sql);
			rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			String columnName = rsmd.getColumnLabel(1);
			while (rs.next()) {
				list.add(rs.getObject(columnName));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ps != null)
					ps.close();
				if (connection != null)
					connection.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return list;
	}

	/**
	 * 获取多列数据
	 * 
	 * @param dbName-数据库名称
	 * @param sql-语句
	 * @return
	 */
	public static List<Map<String, Object>> getListMap(String dbName, String sql) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			connection = dataSourceMap.get(dbName).getConnection();
			ps = connection.prepareStatement(sql);
			rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			// 获取字段
			int columnCount = rsmd.getColumnCount();
			List<String> columnName = new ArrayList<String>();
			for (int i = 1; i <= columnCount; i++) {
				columnName.add(rsmd.getColumnLabel(i));
			}
			// 获取数据
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (String name : columnName)
					map.put(name, rs.getObject(name));
				list.add(map);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ps != null)
					ps.close();
				if (connection != null)
					connection.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return list;
	}
	
	/**
	 * 执行多条语句（前几条事务，最后一条查询）
	 * 
	 * @param dbName-数据库名称
	 * @param sqlList-语句数组
	 * @return
	 */
	public static List<Map<String, Object>> getListMapByBatch(String dbName, List<String> sqlList) {
		if (sqlList.size() < 2)
			return null;
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			connection = dataSourceMap.get(dbName).getConnection();
			connection.setAutoCommit(false);
			Statement statement = connection.createStatement();
			for (String sql : sqlList.subList(0, sqlList.size() - 1)) {
				statement.addBatch(sql);
			}
			statement.executeBatch();
			rs = statement.executeQuery(sqlList.get(sqlList.size() - 1));
			ResultSetMetaData rsmd = rs.getMetaData();
			connection.commit();
			// 获取字段
			int columnCount = rsmd.getColumnCount();
			List<String> columnName = new ArrayList<String>();
			for (int i = 1; i <= columnCount; i++) {
				columnName.add(rsmd.getColumnLabel(i));
			}
			// 获取数据
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (String name : columnName)
					map.put(name, rs.getObject(name));
				list.add(map);
			}
			return list;
		} catch (Exception e) {
			if (connection != null)
				try {
					connection.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			e.printStackTrace();
		} finally {
			try {
				if (ps != null)
					ps.close();
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	/**
	 * 获取ResultSet
	 *
	 * @param dbName-数据库名称
	 * @param sql-语句
	 * @return
	 */
	public static ResultSetDef getResultSet(String dbName, String sql) {
		Connection connection = null;
		List<String> columnName = new ArrayList<String>();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			connection = dataSourceMap.get(dbName).getConnection();
			ps = connection.prepareStatement(sql);
			rs = ps.executeQuery();

			ResultSetMetaData rsmd = rs.getMetaData();
			// 获取字段
			int columnCount = rsmd.getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				columnName.add(rsmd.getColumnLabel(i));
			}
			return new ResultSetDef(connection, ps, rs, columnName);
		} catch (Exception e) {
			e.printStackTrace();
			try {
				if (rs != null)
					rs.close();
				if (ps != null)
					ps.close();
				if (connection != null)
					connection.close();
			} catch (Exception e1) {
				e1.printStackTrace();
			}

		}
		return null;
	}

	/**
	 * 执行单条语句
	 * 
	 * @param dbName-数据库名称
	 * @param sql-语句
	 * @return
	 */
	public static boolean execute(String dbName, String sql) {
		Connection connection = null;
		PreparedStatement ps = null;
		try {
			connection = dataSourceMap.get(dbName).getConnection();
			ps = connection.prepareStatement(sql);
			ps.execute();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				if (ps != null)
					ps.close();
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 执行单条语句，并返回自增键id
	 * 
	 * @param dbName-数据库名称
	 * @param sql-语句
	 * @return
	 */
	public static Integer executeWithId(String dbName, String sql) {
		Connection connection = null;
		PreparedStatement ps = null;
		try {
			connection = dataSourceMap.get(dbName).getConnection();
			ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			ps.execute();
			ResultSet rs = ps.getGeneratedKeys();
			int i = 0;
			if (rs.next()) {
				i = rs.getInt(1);
			}
			return i;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				if (ps != null)
					ps.close();
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 执行多条语句（事务）
	 * 
	 * @param dbName-数据库名称
	 * @param sqlList-语句数组
	 * @return
	 */
	public static boolean executeBatch(String dbName, List<String> sqlList) {
		Connection connection = null;
		PreparedStatement ps = null;
		try {
			connection = dataSourceMap.get(dbName).getConnection();
			connection.setAutoCommit(false);
			Statement statement = connection.createStatement();
			for (String sql : sqlList) {
				statement.addBatch(sql);
			}
			statement.executeBatch();
			connection.commit();
			return true;
		} catch (Exception e) {
			if (connection != null)
				try {
					connection.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			e.printStackTrace();
			return false;
		} finally {
			try {
				if (ps != null)
					ps.close();
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 批量插入数据-适用于MySQL
	 * 
	 * @param dbName-数据库名称
	 * @param tableName-表名
	 * @param content-数据内容
	 * @param isReplace-insert或replace
	 * @return
	 */
	public static boolean insertListMap(String dbName, String tableName, List<Map<String, Object>> content, boolean isReplace) {
		if (content.size() == 0)
			return true;
		String sql = combineInsert(tableName, content, isReplace);
		return execute(dbName, sql);
	}

	/**
	 * 批量插入数据并返回最后一条自增id
	 * 
	 * @param dbName-数据库名称
	 * @param tableName-表名
	 * @param content-数据内容
	 * @param isReplace-insert或replace
	 * @return
	 */
	public static Integer insertListMapWithId(String dbName, String tableName, List<Map<String, Object>> content, boolean isReplace) {
		String sql = combineInsert(tableName, content, isReplace);
		return executeWithId(dbName, sql);
	}

	private static String combineInsert(String tableName, List<Map<String, Object>> content, boolean isReplace) {
		StringBuffer sqlBuf = new StringBuffer();
		Set<String> keySet = content.get(0).keySet();
		List<String> keyList = new ArrayList<String>();
		for (Iterator<String> it = keySet.iterator(); it.hasNext();) {
			keyList.add((String) it.next());
		}
		if (isReplace) {
			sqlBuf.append("replace into ");
			sqlBuf.append(tableName);
			sqlBuf.append(" (");
		} else {
			sqlBuf.append("insert into ");
			sqlBuf.append(tableName);
			sqlBuf.append(" (");
		}
		for (int i = 0; i < keyList.size(); i++) {
			sqlBuf.append(keyList.get(i));
			sqlBuf.append(",");
		}
		sqlBuf.deleteCharAt(sqlBuf.length() - 1);
		sqlBuf.append(") values ");
		for (int i = 0; i < content.size(); i++) {
			sqlBuf.append("(");
			for (int j = 0; j < keyList.size(); j++) {
				if (content.get(i).get(keyList.get(j)) == null)
					sqlBuf.append("null,");
				else if (content.get(i).get(keyList.get(j)).equals("null"))
					sqlBuf.append("null,");
				else if (content.get(i).get(keyList.get(j)).equals("now()"))
					sqlBuf.append("now(),");
				else {
					sqlBuf.append("'");
					sqlBuf.append(content.get(i).get(keyList.get(j)));
					sqlBuf.append("',");
				}
			}
			sqlBuf.deleteCharAt(sqlBuf.length() - 1);
			sqlBuf.append("),");
		}
		sqlBuf.deleteCharAt(sqlBuf.length() - 1);
		return sqlBuf.toString();
	}

	/**
	 * 批量upsert数据-适用于Postgre
	 * 
	 * @param dbName-数据库名称
	 * @param tableName-表名
	 * @param content-数据内容
	 * @param isDistinct-会把相同内容的map进行合并
	 * @return
	 */
	public static boolean upsertListMap(String dbName, String tableName, List<Map<String, Object>> content) {
		if (content.size() == 0)
			return true;
		String sql = combineUpsert(tableName, content);
		// System.out.println(sql);
		return execute(dbName, sql);
	}

	private static final String DOUBLEMARKS = "\"";
	private static final String COMMA = ",";

	private static String combineUpsert(String tableName, List<Map<String, Object>> content) {
		StringBuffer sqlBuf = new StringBuffer();
		Set<String> keySet = content.get(0).keySet();
		List<String> keyList = new ArrayList<String>();
		for (Iterator<String> it = keySet.iterator(); it.hasNext();) {
			keyList.add((String) it.next());
		}
		sqlBuf.append("insert into ");
		sqlBuf.append(tableName);
		sqlBuf.append(" (");
		for (int i = 0; i < keyList.size(); i++) {
			sqlBuf.append(DOUBLEMARKS + keyList.get(i) + DOUBLEMARKS);
			sqlBuf.append(COMMA);
		}
		sqlBuf.deleteCharAt(sqlBuf.length() - 1);
		sqlBuf.append(") values ");
		for (int i = 0; i < content.size(); i++) {
			sqlBuf.append("(");
			for (int j = 0; j < keyList.size(); j++) {
				if (content.get(i).get(keyList.get(j)) == null)
					sqlBuf.append("null,");
				else if (content.get(i).get(keyList.get(j)).equals("null"))
					sqlBuf.append("null,");
				else if (content.get(i).get(keyList.get(j)).equals("now()"))
					sqlBuf.append("now(),");
				else {
					sqlBuf.append("'");
					sqlBuf.append(content.get(i).get(keyList.get(j)));
					sqlBuf.append("',");
				}
			}
			sqlBuf.deleteCharAt(sqlBuf.length() - 1);
			sqlBuf.append("),");
		}
		sqlBuf.deleteCharAt(sqlBuf.length() - 1);
		sqlBuf.append("ON CONFLICT ON CONSTRAINT " + tableName + "_pkey DO update set ");
		for (int i = 0; i < keyList.size(); i++) {
			sqlBuf.append(DOUBLEMARKS + keyList.get(i) + DOUBLEMARKS + "=EXCLUDED." + keyList.get(i) + COMMA);
		}
		sqlBuf.deleteCharAt(sqlBuf.length() - 1);
		return sqlBuf.toString();
	}

	/**
	 * 更新语句
	 * 
	 * @param dbName-数据库名称
	 * @param tableName-表名
	 * @param condition-条件
	 * @param content-数据内容
	 * @return
	 */
	public static boolean update(String dbName, String tableName, Map<String, String> condition, Map<String, Object> content) {
		StringBuffer updateSB = new StringBuffer();
		updateSB.append("update ");
		updateSB.append(tableName);
		updateSB.append(" set ");
		if (condition.size() < 1 || content.size() < 1)
			return false;
		for (String key : content.keySet()) {
			updateSB.append(key);
			updateSB.append("='");
			updateSB.append(content.get(key));
			updateSB.append("',");
		}
		updateSB.deleteCharAt(updateSB.length() - 1);
		updateSB.append(" where ");
		for (String key : condition.keySet()) {
			updateSB.append(key);
			updateSB.append("='");
			updateSB.append(condition.get(key));
			updateSB.append("' and ");
		}
		String sql = updateSB.substring(0, updateSB.length() - 4);
		return execute(dbName, sql);
	}

	/**
	 * 导出csv
	 * 
	 * @param dbName-数据库名称
	 * @param sql-语句
	 * @return
	 */
	public static String exportCsv(String dbName, String sql) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			StringBuffer sb = new StringBuffer();
			connection = dataSourceMap.get(dbName).getConnection();
			ps = connection.prepareStatement(sql);
			rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			// 获取字段
			int columnCount = rsmd.getColumnCount();
			List<String> columnName = new ArrayList<String>();
			for (int i = 1; i <= columnCount; i++) {
				sb.append(rsmd.getColumnLabel(i));
				sb.append(",");
				columnName.add(rsmd.getColumnLabel(i));
			}
			sb.deleteCharAt(sb.length() - 1);
			sb.append("\r\n");
			// 获取数据
			while (rs.next()) {
				for (String name : columnName) {
					sb.append(replaceNull(rs.getString(name)));
					sb.append(",");
				}
				sb.deleteCharAt(sb.length() - 1);
				sb.append("\r\n");
			}
			return sb.substring(0, sb.length() - 2);
		} catch (Exception e) {
			e.printStackTrace();
			return "";
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ps != null)
					ps.close();
				if (connection != null)
					connection.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 导入csv，第一行为字段名
	 * 
	 * @param dbName-数据库名称
	 * @param tableName-表名
	 * @param content-csv文件内容
	 * @param isReplace-insert或replace
	 * @return
	 */
	public static boolean importCsv(String dbName, String tableName, String content, boolean isReplace) {
		String[] contentArray = content.split("\r\n");
		if (contentArray.length < 2)
			return false;
		StringBuffer sqlBuf = new StringBuffer();
		if (isReplace) {
			sqlBuf.append("replace into ");
			sqlBuf.append(tableName);
			sqlBuf.append(" (");
		} else {
			sqlBuf.append("insert into ");
			sqlBuf.append(tableName);
			sqlBuf.append(" (");
		}
		sqlBuf.append(contentArray[0]);
		sqlBuf.append(") values ");
		for (int i = 1; i < contentArray.length; i++) {
			String temp = "('" + contentArray[i].replaceAll(",", "','") + "'),";
			sqlBuf.append(temp);
		}
		sqlBuf.deleteCharAt(sqlBuf.length() - 1);
		if (executeWithId(dbName, sqlBuf.toString()) == null)
			return false;
		return true;
	}

	private static String replaceNull(String str) {
		if (str == null)
			return "null";
		else
			return str;
	}
}
