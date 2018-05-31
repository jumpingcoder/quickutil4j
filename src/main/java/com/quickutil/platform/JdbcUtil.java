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

import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.quickutil.platform.def.ResultSetDef;

import ch.qos.logback.classic.Logger;

@Deprecated
public class JdbcUtil {

	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(JdbcUtil.class);

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
	 * @param pool-c3p0配置
	 */
	public static void addDataSource(Properties jdbc, Properties pool) {
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
				ComboPooledDataSource datasource = buildDataSource(key, jdbcUrl, user, password, initconnum, minconnum, maxconnum, pool);
				dataSourceMap.put(key, datasource);
				// JsonUtil.toJson(datasource.getConnection().getMetaData().getDatabaseProductName());
				// JsonUtil.toJson(datasource.getConnection().getMetaData().getDatabaseMajorVersion());
			} catch (Exception e) {
				LOGGER.error("",e);
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
		dataSourceMap.put(dbName, buildDataSource(dbName, url, username, password, initconnum, minconnum, maxconnum, null));
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
	 * @param pool-c3p0配置
	 */
	public static void addDataSource(String dbName, String url, String username, String password, int initconnum, int minconnum, int maxconnum, Properties pool) {
		dataSourceMap.put(dbName, buildDataSource(dbName, url, username, password, initconnum, minconnum, maxconnum, pool));
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

	/**
	 * 释放生成的DataSource
	 * 
	 * @param dbName
	 */
	public void closeDataSource(String dbName) {
		ComboPooledDataSource pool = dataSourceMap.get(dbName);
		if (pool == null)
			return;
		pool.close();
		dataSourceMap.remove(dbName);
	}

	/**
	 * 释放所有生成的DataSource
	 * 
	 */
	public void closeAllDataSource() {
		for (String dbName : dataSourceMap.keySet()) {
			closeDataSource(dbName);
		}
	}

	private static ComboPooledDataSource buildDataSource(String dbName, String jdbcUrl, String username, String password, int initconnum, int minconnum, int maxconnum, Properties pool) {
		if (jdbcUrl == null || username == null || password == null) {
			return null;
		}
		ComboPooledDataSource datasource = new ComboPooledDataSource();
		try {
			datasource.setDataSourceName(dbName);
			datasource.setDriverClass("com.mysql.jdbc.Driver");
			datasource.setJdbcUrl(jdbcUrl);
			datasource.setUser(username);
			datasource.setPassword(password);
			datasource.setInitialPoolSize(initconnum);
			datasource.setMinPoolSize(minconnum);
			datasource.setMaxPoolSize(maxconnum);
			if (pool == null)
				return datasource;
			if (pool.getProperty("DriverClass") != null)
				datasource.setDriverClass(pool.getProperty("DriverClass"));
			if (pool.getProperty("AcquireIncrement") != null)
				datasource.setAcquireIncrement(Integer.parseInt(pool.getProperty("AcquireIncrement")));
			if (pool.getProperty("AcquireRetryAttempts") != null)
				datasource.setAcquireRetryAttempts(Integer.parseInt(pool.getProperty("AcquireRetryAttempts")));
			if (pool.getProperty("AcquireRetryDelay") != null)
				datasource.setAcquireRetryDelay(Integer.parseInt(pool.getProperty("AcquireRetryDelay")));
			if (pool.getProperty("AutoCommitOnClose") != null)
				datasource.setAutoCommitOnClose(Boolean.parseBoolean(pool.getProperty("AutoCommitOnClose")));
			if (pool.getProperty("AutomaticTestTable") != null)
				datasource.setAutomaticTestTable(pool.getProperty("AutomaticTestTable"));
			if (pool.getProperty("BreakAfterAcquireFailure") != null)
				datasource.setBreakAfterAcquireFailure(Boolean.parseBoolean(pool.getProperty("BreakAfterAcquireFailure")));
			if (pool.getProperty("CheckoutTimeout") != null)
				datasource.setCheckoutTimeout(Integer.parseInt(pool.getProperty("CheckoutTimeout")));
			if (pool.getProperty("ConnectionCustomizerClassName") != null)
				datasource.setConnectionCustomizerClassName(pool.getProperty("ConnectionCustomizerClassName"));
			if (pool.getProperty("ConnectionTesterClassName") != null)
				datasource.setConnectionTesterClassName(pool.getProperty("ConnectionTesterClassName"));
			if (pool.getProperty("ContextClassLoaderSource") != null)
				datasource.setContextClassLoaderSource(pool.getProperty("ContextClassLoaderSource"));
			if (pool.getProperty("DataSourceName") != null)
				datasource.setDataSourceName(pool.getProperty("DataSourceName"));
			if (pool.getProperty("DebugUnreturnedConnectionStackTraces") != null)
				datasource.setDebugUnreturnedConnectionStackTraces(Boolean.parseBoolean(pool.getProperty("DebugUnreturnedConnectionStackTraces")));
			if (pool.getProperty("Description") != null)
				datasource.setDescription(pool.getProperty("Description"));
			if (pool.getProperty("FactoryClassLocation") != null)
				datasource.setFactoryClassLocation(pool.getProperty("FactoryClassLocation"));
			if (pool.getProperty("ForceIgnoreUnresolvedTransactions") != null)
				datasource.setForceIgnoreUnresolvedTransactions(Boolean.parseBoolean(pool.getProperty("ForceIgnoreUnresolvedTransactions")));
			if (pool.getProperty("ForceSynchronousCheckins") != null)
				datasource.setForceSynchronousCheckins(Boolean.parseBoolean(pool.getProperty("ForceSynchronousCheckins")));
			if (pool.getProperty("ForceUseNamedDriverClass") != null)
				datasource.setForceUseNamedDriverClass(Boolean.parseBoolean(pool.getProperty("ForceUseNamedDriverClass")));
			if (pool.getProperty("IdentityToken") != null)
				datasource.setIdentityToken(pool.getProperty("IdentityToken"));
			if (pool.getProperty("IdleConnectionTestPeriod") != null)
				datasource.setIdleConnectionTestPeriod(Integer.parseInt(pool.getProperty("IdleConnectionTestPeriod")));
			if (pool.getProperty("LoginTimeout") != null)
				datasource.setLoginTimeout(Integer.parseInt(pool.getProperty("LoginTimeout")));
			if (pool.getProperty("MaxAdministrativeTaskTime") != null)
				datasource.setMaxAdministrativeTaskTime(Integer.parseInt(pool.getProperty("MaxAdministrativeTaskTime")));
			if (pool.getProperty("MaxConnectionAge") != null)
				datasource.setMaxConnectionAge(Integer.parseInt(pool.getProperty("MaxConnectionAge")));
			if (pool.getProperty("MaxIdleTime") != null)
				datasource.setMaxIdleTime(Integer.parseInt(pool.getProperty("MaxIdleTime")));
			if (pool.getProperty("MaxStatements") != null)
				datasource.setMaxStatements(Integer.parseInt(pool.getProperty("MaxStatements")));
			if (pool.getProperty("MaxStatementsPerConnection") != null)
				datasource.setMaxStatementsPerConnection(Integer.parseInt(pool.getProperty("MaxStatementsPerConnection")));
			if (pool.getProperty("NumHelperThreads") != null)
				datasource.setNumHelperThreads(Integer.parseInt(pool.getProperty("NumHelperThreads")));
			if (pool.getProperty("OverrideDefaultPassword") != null)
				datasource.setOverrideDefaultPassword(pool.getProperty("OverrideDefaultPassword"));
			if (pool.getProperty("OverrideDefaultUser") != null)
				datasource.setOverrideDefaultUser(pool.getProperty("OverrideDefaultUser"));
			if (pool.getProperty("PreferredTestQuery") != null)
				datasource.setPreferredTestQuery(pool.getProperty("PreferredTestQuery"));
			if (pool.getProperty("PrivilegeSpawnedThreads") != null)
				datasource.setPrivilegeSpawnedThreads(Boolean.parseBoolean(pool.getProperty("PrivilegeSpawnedThreads")));
			if (pool.getProperty("PropertyCycle") != null)
				datasource.setPropertyCycle(Integer.parseInt(pool.getProperty("PropertyCycle")));
			if (pool.getProperty("StatementCacheNumDeferredCloseThreads") != null)
				datasource.setStatementCacheNumDeferredCloseThreads(Integer.parseInt(pool.getProperty("StatementCacheNumDeferredCloseThreads")));
			if (pool.getProperty("TestConnectionOnCheckin") != null)
				datasource.setTestConnectionOnCheckin(Boolean.parseBoolean(pool.getProperty("TestConnectionOnCheckin")));
			if (pool.getProperty("TestConnectionOnCheckout") != null)
				datasource.setTestConnectionOnCheckout(Boolean.parseBoolean(pool.getProperty("TestConnectionOnCheckout")));
			if (pool.getProperty("UnreturnedConnectionTimeout") != null)
				datasource.setUnreturnedConnectionTimeout(Integer.parseInt(pool.getProperty("UnreturnedConnectionTimeout")));
			if (pool.getProperty("UserOverridesAsString") != null)
				datasource.setUserOverridesAsString(pool.getProperty("UserOverridesAsString"));
			if (pool.getProperty("UsesTraditionalReflectiveProxies") != null)
				datasource.setUsesTraditionalReflectiveProxies(Boolean.parseBoolean(pool.getProperty("UsesTraditionalReflectiveProxies")));
		} catch (Exception e) {
			LOGGER.error("",e);
		}
		return datasource;
	}

	/**
	 * 用于输入格式化，避免sql注入
	 * 
	 * @param sql-SQL语句
	 * @param escape-SQL转义符，使用''替换'
	 * @param params-参数，数组只支持string[]，格式会被转义成'a','b','c','d'
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static String format(String sql, String escape, Object... params) {
		for (int i = 0; i < params.length; i++) {
			if (params[i] instanceof List) {
				StringBuilder sb = new StringBuilder();
				List<String> array = (List<String>) params[i];
				for (String sn : array)
					sb.append("'" + sn.replaceAll("'", escape + "'") + "',");
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
			LOGGER.error("",e);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ps != null)
					ps.close();
				if (connection != null)
					connection.close();
			} catch (Exception e) {
				LOGGER.error("",e);
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
		List<Map<String, Object>> list = new ArrayList<>();
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
				Map<String, Object> map = new HashMap<>();
				for (String name : columnName)
					map.put(name, rs.getObject(name));
				list.add(map);
			}
		} catch (Exception e) {
			LOGGER.error("",e);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ps != null)
					ps.close();
				if (connection != null)
					connection.close();
			} catch (Exception e) {
				LOGGER.error("",e);
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
		List<Map<String, Object>> list = new ArrayList<>();
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
				Map<String, Object> map = new HashMap<>();
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
			LOGGER.error("",e);
		} finally {
			try {
				if (ps != null)
					ps.close();
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				LOGGER.error("",e);
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
			LOGGER.error("",e);
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
			LOGGER.error("",e);
			return false;
		} finally {
			try {
				if (ps != null)
					ps.close();
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				LOGGER.error("",e);
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
			LOGGER.error("",e);
			return null;
		} finally {
			try {
				if (ps != null)
					ps.close();
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				LOGGER.error("",e);
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
			LOGGER.error("",e);
			return false;
		} finally {
			try {
				if (ps != null)
					ps.close();
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				LOGGER.error("",e);
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
		boolean result = execute(dbName, sql);
		if (!result)
			LOGGER.warn(sql);
		return result;
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
		boolean result = execute(dbName, sql);
		if (!result) {
			LOGGER.warn(sql);
		}
		return false;
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
			LOGGER.error("",e);
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
				LOGGER.error("",e);
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
