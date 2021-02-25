package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.alibaba.druid.pool.DruidDataSource;
import com.quickutil.platform.constants.Symbol;
import com.quickutil.platform.entity.ResultSetDef;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;

/**
 * 关系型数据库Jdbc工具
 *
 * @author 0.5
 */
public class JdbcUtil {

	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(JdbcUtil.class);
	private static final String DOUBLEMARKS = "\"";
	private DruidDataSource datasource;

	public JdbcUtil(String dbName, String jdbcUrl, String username, String password, int initconnum, int minconnum, int maxconnum, Properties druidProperties) {
		datasource = buildDataSource(dbName, jdbcUrl, username, password, initconnum, minconnum, maxconnum, druidProperties);
	}

	public DruidDataSource getDataSource() {
		return datasource;
	}

	public void closeDataSource() {
		if (datasource == null)
			return;
		datasource.close();
	}


	private DruidDataSource buildDataSource(String dbName, String jdbcUrl, String username, String password, int initconnum, int minconnum, int maxconnum, Properties druidProperties) {
		if (jdbcUrl == null || username == null || password == null) {
			return null;
		}
		DruidDataSource datasource = new DruidDataSource();
		try {
			datasource.setName(dbName);
			datasource.setUrl(jdbcUrl);
			datasource.setUsername(username);
			datasource.setPassword(password);
			datasource.setInitialSize(initconnum);
			datasource.setMinIdle(minconnum);
			datasource.setMaxActive(maxconnum);
			if (minconnum < 1) {
				LOGGER.warn("The minimum number of connections is less than one");
			}
			if (maxconnum > Runtime.getRuntime().availableProcessors() * 2) {
				LOGGER.warn("The maximum number of connections is more than twice the number of CPU cores");
			}
			if (druidProperties == null) {
				return datasource;
			}
			if (druidProperties.getProperty("DriverClass") != null) {
				datasource.setDriverClassName((druidProperties.getProperty("DriverClass")));
			}
			if (druidProperties.getProperty("AccessToUnderlyingConnectionAllowed") != null) {
				datasource.setAccessToUnderlyingConnectionAllowed(Boolean.parseBoolean(druidProperties.getProperty("AccessToUnderlyingConnectionAllowed")));
			}
			if (druidProperties.getProperty("AsyncCloseConnectionEnable") != null) {
				datasource.setAsyncCloseConnectionEnable(Boolean.parseBoolean(druidProperties.getProperty("AsyncCloseConnectionEnable")));
			}
			if (druidProperties.getProperty("BreakAfterAcquireFailure") != null) {
				datasource.setBreakAfterAcquireFailure(Boolean.parseBoolean(druidProperties.getProperty("BreakAfterAcquireFailure")));
			}
			if (druidProperties.getProperty("ClearFiltersEnable") != null) {
				datasource.setClearFiltersEnable(Boolean.parseBoolean(druidProperties.getProperty("ClearFiltersEnable")));
			}
			if (druidProperties.getProperty("ConnectionErrorRetryAttempts") != null) {
				datasource.setConnectionErrorRetryAttempts(Integer.parseInt(druidProperties.getProperty("ConnectionErrorRetryAttempts")));
			}
			if (druidProperties.getProperty("DbType") != null) {
				datasource.setDbType(druidProperties.getProperty("DbType"));
			}
			if (druidProperties.getProperty("DefaultAutoCommit") != null) {
				datasource.setDefaultAutoCommit(Boolean.parseBoolean(druidProperties.getProperty("DefaultAutoCommit")));
			}
			if (druidProperties.getProperty("DefaultCatalog") != null) {
				datasource.setDefaultCatalog(druidProperties.getProperty("DefaultCatalog"));
			}
			if (druidProperties.getProperty("DefaultReadOnly") != null) {
				datasource.setDefaultReadOnly(Boolean.parseBoolean(druidProperties.getProperty("DefaultReadOnly")));
			}
			if (druidProperties.getProperty("DefaultTransactionIsolation") != null) {
				datasource.setDefaultTransactionIsolation(Integer.parseInt(druidProperties.getProperty("DefaultTransactionIsolation")));
			}
			if (druidProperties.getProperty("Enable") != null) {
				datasource.setEnable(Boolean.parseBoolean(druidProperties.getProperty("Enable")));
			}
			if (druidProperties.getProperty("FailFast") != null) {
				datasource.setFailFast(Boolean.parseBoolean(druidProperties.getProperty("FailFast")));
			}
			if (druidProperties.getProperty("InitGlobalVariants") != null) {
				datasource.setInitGlobalVariants(Boolean.parseBoolean(druidProperties.getProperty("InitGlobalVariants")));
			}
			if (druidProperties.getProperty("InitVariants") != null) {
				datasource.setInitVariants(Boolean.parseBoolean(druidProperties.getProperty("InitVariants")));
			}
			if (druidProperties.getProperty("KeepAlive") != null) {
				datasource.setKeepAlive(Boolean.parseBoolean(druidProperties.getProperty("KeepAlive")));
			}
			if (druidProperties.getProperty("KillWhenSocketReadTimeout") != null) {
				datasource.setKillWhenSocketReadTimeout(Boolean.parseBoolean(druidProperties.getProperty("KillWhenSocketReadTimeout")));
			}
			if (druidProperties.getProperty("LogAbandoned") != null) {
				datasource.setLogAbandoned(Boolean.parseBoolean(druidProperties.getProperty("LogAbandoned")));
			}
			if (druidProperties.getProperty("LogDifferentThread") != null) {
				datasource.setLogDifferentThread(Boolean.parseBoolean(druidProperties.getProperty("LogDifferentThread")));
			}
			if (druidProperties.getProperty("LoginTimeout") != null) {
				datasource.setLoginTimeout(Integer.parseInt(druidProperties.getProperty("LoginTimeout")));
			}
			if (druidProperties.getProperty("MaxCreateTaskCount") != null) {
				datasource.setMaxCreateTaskCount(Integer.parseInt(druidProperties.getProperty("MaxCreateTaskCount")));
			}
			if (druidProperties.getProperty("MaxEvictableIdleTimeMillis") != null) {
				datasource.setMaxEvictableIdleTimeMillis(Long.parseLong(druidProperties.getProperty("MaxEvictableIdleTimeMillis")));
			}
			if (druidProperties.getProperty("MaxOpenPreparedStatements") != null) {
				datasource.setMaxOpenPreparedStatements(Integer.parseInt(druidProperties.getProperty("MaxOpenPreparedStatements")));
			}
			if (druidProperties.getProperty("PoolPreparedStatementPerConnectionSize") != null) {
				datasource.setMaxPoolPreparedStatementPerConnectionSize(Integer.parseInt(druidProperties.getProperty("PoolPreparedStatementPerConnectionSize")));
			}
			if (druidProperties.getProperty("MaxWait") != null) {
				datasource.setMaxWait(Long.parseLong(druidProperties.getProperty("MaxWait")));
			}
			if (druidProperties.getProperty("MaxWaitThreadCount") != null) {
				datasource.setMaxWaitThreadCount(Integer.parseInt(druidProperties.getProperty("MaxWaitThreadCount")));
			}
			if (druidProperties.getProperty("MinEvictableIdleTimeMillis") != null) {
				datasource.setMinEvictableIdleTimeMillis(Long.parseLong(druidProperties.getProperty("MinEvictableIdleTimeMillis")));
			}
			if (druidProperties.getProperty("MinIdle") != null) {
				datasource.setMinIdle(Integer.parseInt(druidProperties.getProperty("MinIdle")));
			}
			if (druidProperties.getProperty("NotFullTimeoutRetryCount") != null) {
				datasource.setNotFullTimeoutRetryCount(Integer.parseInt(druidProperties.getProperty("NotFullTimeoutRetryCount")));
			}
			if (druidProperties.getProperty("OnFatalErrorMaxActive") != null) {
				datasource.setOnFatalErrorMaxActive(Integer.parseInt(druidProperties.getProperty("OnFatalErrorMaxActive")));
			}
			if (druidProperties.getProperty("Oracle") != null) {
				datasource.setOracle(Boolean.parseBoolean(druidProperties.getProperty("Oracle")));
			}
			if (druidProperties.getProperty("PoolPreparedStatements") != null) {
				datasource.setPoolPreparedStatements(Boolean.parseBoolean(druidProperties.getProperty("PoolPreparedStatements")));
			}
			if (druidProperties.getProperty("QueryTimeout") != null) {
				datasource.setQueryTimeout(Integer.parseInt(druidProperties.getProperty("QueryTimeout")));
			}
			if (druidProperties.getProperty("RemoveAbandoned") != null) {
				datasource.setRemoveAbandoned(Boolean.parseBoolean(druidProperties.getProperty("RemoveAbandoned")));
			}
			if (druidProperties.getProperty("RemoveAbandonedTimeout") != null) {
				datasource.setRemoveAbandonedTimeout(Integer.parseInt(druidProperties.getProperty("RemoveAbandonedTimeout")));
			}
			if (druidProperties.getProperty("RemoveAbandonedTimeoutMillis") != null) {
				datasource.setRemoveAbandonedTimeoutMillis(Long.parseLong(druidProperties.getProperty("RemoveAbandonedTimeoutMillis")));
			}
			if (druidProperties.getProperty("ResetStatEnable") != null) {
				datasource.setResetStatEnable(Boolean.parseBoolean(druidProperties.getProperty("ResetStatEnable")));
			}
			if (druidProperties.getProperty("SharePreparedStatements") != null) {
				datasource.setSharePreparedStatements(Boolean.parseBoolean(druidProperties.getProperty("SharePreparedStatements")));
			}
			if (druidProperties.getProperty("TestOnBorrow") != null) {
				datasource.setTestOnBorrow(Boolean.parseBoolean(druidProperties.getProperty("TestOnBorrow")));
			}
			if (druidProperties.getProperty("TestOnReturn") != null) {
				datasource.setTestOnReturn(Boolean.parseBoolean(druidProperties.getProperty("TestOnReturn")));
			}
			if (druidProperties.getProperty("TimeBetweenConnectErrorMillis") != null) {
				datasource.setTimeBetweenConnectErrorMillis(Long.parseLong(druidProperties.getProperty("TimeBetweenConnectErrorMillis")));
			}
			if (druidProperties.getProperty("TimeBetweenEvictionRunsMillis") != null) {
				datasource.setTimeBetweenEvictionRunsMillis(Long.parseLong(druidProperties.getProperty("TimeBetweenEvictionRunsMillis")));
			}
			if (druidProperties.getProperty("TimeBetweenLogStatsMillis") != null) {
				datasource.setTimeBetweenLogStatsMillis(Long.parseLong(druidProperties.getProperty("TimeBetweenLogStatsMillis")));
			}
			if (druidProperties.getProperty("TransactionQueryTimeout") != null) {
				datasource.setTransactionQueryTimeout(Integer.parseInt(druidProperties.getProperty("TransactionQueryTimeout")));
			}
			if (druidProperties.getProperty("TransactionThresholdMillis") != null) {
				datasource.setTransactionThresholdMillis(Long.parseLong(druidProperties.getProperty("TransactionThresholdMillis")));
			}
			if (druidProperties.getProperty("UseGlobalDataSourceStat") != null) {
				datasource.setUseGlobalDataSourceStat(Boolean.parseBoolean(druidProperties.getProperty("UseGlobalDataSourceStat")));
			}
			if (druidProperties.getProperty("UseLocalSessionState") != null) {
				datasource.setUseLocalSessionState(Boolean.parseBoolean(druidProperties.getProperty("UseLocalSessionState")));
			}
			if (druidProperties.getProperty("UseOracleImplicitCache") != null) {
				datasource.setUseOracleImplicitCache(Boolean.parseBoolean(druidProperties.getProperty("UseOracleImplicitCache")));
			}
			if (druidProperties.getProperty("UseUnfairLock") != null) {
				datasource.setUseUnfairLock(Boolean.parseBoolean(druidProperties.getProperty("UseUnfairLock")));
			}
		} catch (Exception e) {
			LOGGER.error(Symbol.BLANK, e);
		}
		return datasource;
	}

	/**
	 * 用于输入格式化，避免sql注入
	 */
	@SuppressWarnings("unchecked")
	public static String format(String sql, Object... params) {
		for (int i = 0; i < params.length; i++) {
			if (params[i] instanceof List) {
				StringBuilder sb = new StringBuilder();
				List<String> array = (List<String>) params[i];
				for (String str : array) {
					sb.append("'" + str.replaceAll("'", "''") + "',");
				}
				params[i] = sb.substring(0, sb.length() - 1);
			} else {
				params[i] = params[i].toString().replaceAll("'", "''");
			}
		}
		return String.format(sql, params);
	}

	/**
	 * 获取单列数据
	 */
	public List<Object> getListSingle(String sql) {
		List<Object> list = new ArrayList<Object>();
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			connection = datasource.getConnection();
			ps = connection.prepareStatement(sql);
			rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			String columnName = rsmd.getColumnLabel(1);
			while (rs.next()) {
				list.add(rs.getObject(columnName));
			}
		} catch (Exception e) {
			LOGGER.error(Symbol.BLANK, e);
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ps != null) {
					ps.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
				LOGGER.error(Symbol.BLANK, e);
			}
		}
		return list;
	}

	/**
	 * 返回ListMap，有错误抛出
	 */
	public List<Map<String, Object>> getListMapThrowable(String sql) throws Exception {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Map<String, Object>> list = new ArrayList<>();
		try {
			connection = datasource.getConnection();
			ps = connection.prepareStatement(sql);
			rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			// 获取字段
			int columnCount = rsmd.getColumnCount();
			List<String> columnNames = new ArrayList<String>();
			for (int i = 1; i <= columnCount; i++) {
				columnNames.add(rsmd.getColumnLabel(i));
			}
			// 获取数据
			while (rs.next()) {
				Map<String, Object> map = new TreeMap<>();
				for (String columnName : columnNames) {
					map.put(columnName, rs.getObject(columnName));
				}
				list.add(map);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ps != null) {
					ps.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
				throw e;
			}
		}
		return list;
	}

	/**
	 * 返回ListMap，无错误抛出
	 */
	public List<Map<String, Object>> getListMap(String sql) {
		try {
			return getListMapThrowable(sql);
		} catch (Exception e) {
			LOGGER.error(Symbol.BLANK, e);
		}
		return new ArrayList<>();
	}

	/**
	 * 返回对象数组，有错误抛出
	 */
	public <T> List<T> getListObjectThrowable(String sql, Class<T> clazz) throws Exception {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<T> list = new ArrayList<>();
		try {
			connection = datasource.getConnection();
			ps = connection.prepareStatement(sql);
			rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			// 获取字段
			int columnCount = rsmd.getColumnCount();
			List<String> columnNames = new ArrayList<String>();
			for (int i = 1; i <= columnCount; i++) {
				columnNames.add(rsmd.getColumnLabel(i));
			}
			// 生成映射
			Field[] fields = clazz.getDeclaredFields();
			for (Field field : fields) {
				field.setAccessible(true);
			}
			Map<String, String> mapping = new HashMap<>();
			for (String columnName : columnNames) {
				for (Field field : fields) {
					String columnNameDealed = columnName.replaceAll(Symbol.UNDERSCORE, Symbol.BLANK).toLowerCase();
					String fieldNameDealed = field.getName().replaceAll(Symbol.UNDERSCORE, Symbol.BLANK).toLowerCase();
					if (columnNameDealed.equals(fieldNameDealed)) {
						mapping.put(field.getName(), columnName);
						break;
					}
				}
			}
			// 获取数据
			while (rs.next()) {
				T t = clazz.getConstructor().newInstance();
				for (Field field : fields) {
					if (mapping.get(field.getName()) != null)
						field.set(t, rs.getObject(mapping.get(field.getName())));
				}
				list.add(t);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ps != null) {
					ps.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
				throw e;
			}
		}
		return list;
	}

	/**
	 * 返回对象数组，有错误抛出
	 */
	public <T> List<T> getListObject(String sql, Class<T> clazz) {
		try {
			return getListObjectThrowable(sql, clazz);
		} catch (Exception e) {
			LOGGER.error(Symbol.BLANK, e);
		}
		return new ArrayList<>();
	}

	/**
	 * 返回ListMap，一次执行多条语句
	 */
	public List<Map<String, Object>> getListMapByBatch(List<String> sqlList) {
		if (sqlList.size() < 2) {
			return null;
		}
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Map<String, Object>> list = new ArrayList<>();
		try {
			connection = datasource.getConnection();
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
				for (String name : columnName) {
					map.put(name, rs.getObject(name));
				}
				list.add(map);
			}
			return list;
		} catch (Exception e) {
			if (connection != null) {
				try {
					connection.rollback();
				} catch (SQLException e1) {
					LOGGER.error(Symbol.BLANK, e1);
				}
			}
			LOGGER.error(Symbol.BLANK, e);
		} finally {
			try {
				if (ps != null) {
					ps.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				LOGGER.error(Symbol.BLANK, e);
			}
		}
		return null;
	}

	/**
	 * 返回ListMap，一次执行多条语句
	 */
	public <T> List<T> getListObjectByBatch(List<String> sqlList, Class<T> clazz) {
		if (sqlList.size() < 2) {
			return null;
		}
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<T> list = new ArrayList<>();
		try {
			connection = datasource.getConnection();
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
			List<String> columnNames = new ArrayList<String>();
			for (int i = 1; i <= columnCount; i++) {
				columnNames.add(rsmd.getColumnLabel(i));
			}
			// 生成映射
			Field[] fields = clazz.getDeclaredFields();
			for (Field field : fields) {
				field.setAccessible(true);
			}
			Map<String, String> mapping = new HashMap<>();
			for (String columnName : columnNames) {
				for (Field field : fields) {
					String columnNameDealed = columnName.replaceAll(Symbol.UNDERSCORE, Symbol.BLANK).toLowerCase();
					String fieldNameDealed = field.getName().replaceAll(Symbol.UNDERSCORE, Symbol.BLANK).toLowerCase();
					if (columnNameDealed.equals(fieldNameDealed)) {
						mapping.put(field.getName(), columnName);
						break;
					}
				}
			}
			// 获取数据
			while (rs.next()) {
				T t = clazz.getConstructor().newInstance();
				for (Field field : fields) {
					if (mapping.get(field.getName()) != null)
						field.set(t, rs.getObject(mapping.get(field.getName())));
				}
				list.add(t);
			}
		} catch (Exception e) {
			if (connection != null) {
				try {
					connection.rollback();
				} catch (SQLException e1) {
					LOGGER.error(Symbol.BLANK, e1);
				}
			}
			LOGGER.error(Symbol.BLANK, e);
		} finally {
			try {
				if (ps != null) {
					ps.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				LOGGER.error(Symbol.BLANK, e);
			}
		}
		return null;
	}

	/**
	 * 获取ResultSet，有错误抛出
	 */
	public ResultSetDef getResultSetThrowable(String sql) throws Exception {
		Connection connection = null;
		List<String> columnName = new ArrayList<String>();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			connection = datasource.getConnection();
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
			try {
				if (rs != null) {
					rs.close();
				}
				if (ps != null) {
					ps.close();
				}
				if (connection != null) {
					connection.close();
				}
				throw e;
			} catch (Exception e1) {
				LOGGER.error(Symbol.BLANK, e1);
				throw e;
			}
		}
	}

	/**
	 * 获取ResultSet，无错误抛出
	 */
	public ResultSetDef getResultSet(String sql) {
		try {
			return getResultSetThrowable(sql);
		} catch (Exception e) {
			LOGGER.error(Symbol.BLANK, e);
			return null;
		}
	}

	/**
	 * 执行单条语句
	 */
	public boolean execute(String sql) {
		Connection connection = null;
		PreparedStatement ps = null;
		try {
			connection = datasource.getConnection();
			ps = connection.prepareStatement(sql);
			ps.execute();
			return true;
		} catch (Exception e) {
			LOGGER.error(Symbol.BLANK, e);
			return false;
		} finally {
			try {
				if (ps != null) {
					ps.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				LOGGER.error(Symbol.BLANK, e);
			}
		}
	}

	/**
	 * 执行单条语句，并返回自增键id
	 */
	public Integer executeWithId(String sql) {
		Connection connection = null;
		PreparedStatement ps = null;
		try {
			connection = datasource.getConnection();
			ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			ps.execute();
			ResultSet rs = ps.getGeneratedKeys();
			int i = 0;
			if (rs.next()) {
				i = rs.getInt(1);
			}
			return i;
		} catch (Exception e) {
			LOGGER.error("", e);
			return null;
		} finally {
			try {
				if (ps != null) {
					ps.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				LOGGER.error(Symbol.BLANK, e);
			}
		}
	}

	/**
	 * 执行多条语句（事务）
	 */
	public boolean executeBatch(List<String> sqlList) {
		Connection connection = null;
		PreparedStatement ps = null;
		try {
			connection = datasource.getConnection();
			connection.setAutoCommit(false);
			Statement statement = connection.createStatement();
			for (String sql : sqlList) {
				statement.addBatch(sql);
			}
			statement.executeBatch();
			connection.commit();
			return true;
		} catch (Exception e) {
			if (connection != null) {
				try {
					connection.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
			LOGGER.error(Symbol.BLANK, e);
			return false;
		} finally {
			try {
				if (ps != null) {
					ps.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				LOGGER.error(Symbol.BLANK, e);
			}
		}
	}

	/**
	 * 批量插入数据
	 */
	public boolean insertListMap(String tableName, List<Map<String, Object>> content) {
		if (content.size() == 0) {
			return true;
		}
		String sql = combineInsert(tableName, content, false);
		return execute(sql);
	}

	/**
	 * 批量插入数据，并返回最后一条自增id
	 */
	public Integer insertListMapWithId(String tableName, List<Map<String, Object>> content) {
		if (content.size() == 0) {
			return null;
		}
		String sql = combineInsert(tableName, content, false);
		return executeWithId(sql);
	}

	/**
	 * 批量替换数据-只适用于MySQL
	 */
	public boolean replaceListMapForMySQL(String tableName, List<Map<String, Object>> content) {
		if (content.size() == 0) {
			return true;
		}
		String sql = combineInsert(tableName, content, true);
		return execute(sql);
	}

	/**
	 * 批量替换数据并返回最后一条自增id-只适用于MySQL
	 */
	public Integer replaceListMapWithIdForMySQL(String tableName, List<Map<String, Object>> content) {
		if (content.size() == 0) {
			return null;
		}
		String sql = combineInsert(tableName, content, true);
		return executeWithId(sql);
	}

	/**
	 * 批量upsert数据-适用于Postgre
	 */
	public boolean upsertListMapForPG(String tableName, List<Map<String, Object>> content, String constraint) {
		if (content.size() == 0) {
			return true;
		}
		String sql = combineUpsert(tableName, content, constraint);
		boolean result = execute(sql);
		if (!result) {
			LOGGER.debug(sql);
		}
		return result;
	}

	private String combineInsert(String tableName, List<Map<String, Object>> content, boolean isReplace) {
		StringBuffer sqlBuf = new StringBuffer();
		Set<String> keySet = content.get(0).keySet();
		List<String> keyList = new ArrayList<String>();
		for (Iterator<String> it = keySet.iterator(); it.hasNext(); ) {
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
				if (content.get(i).get(keyList.get(j)) == null) {
					sqlBuf.append("null,");
				} else if (content.get(i).get(keyList.get(j)).equals("null")) {
					sqlBuf.append("null,");
				} else if (content.get(i).get(keyList.get(j)).equals("now()")) {
					sqlBuf.append("now(),");
				} else {
					sqlBuf.append("'");
					sqlBuf.append(content.get(i).get(keyList.get(j)).toString().replaceAll("'", "''"));
					sqlBuf.append("',");
				}
			}
			sqlBuf.deleteCharAt(sqlBuf.length() - 1);
			sqlBuf.append("),");
		}
		sqlBuf.deleteCharAt(sqlBuf.length() - 1);
		return sqlBuf.toString();
	}

	private static String combineUpsert(String tableName, List<Map<String, Object>> content, String constraint) {
		StringBuffer sqlBuf = new StringBuffer();
		Set<String> keySet = content.get(0).keySet();
		List<String> keyList = new ArrayList<String>();
		for (Iterator<String> it = keySet.iterator(); it.hasNext(); ) {
			keyList.add((String) it.next());
		}
		sqlBuf.append("insert into ");
		sqlBuf.append(tableName);
		sqlBuf.append(" (");
		for (int i = 0; i < keyList.size(); i++) {
			sqlBuf.append(DOUBLEMARKS + keyList.get(i) + DOUBLEMARKS);
			sqlBuf.append(Symbol.COMMA);
		}
		sqlBuf.deleteCharAt(sqlBuf.length() - 1);
		sqlBuf.append(") values ");
		for (int i = 0; i < content.size(); i++) {
			sqlBuf.append("(");
			for (int j = 0; j < keyList.size(); j++) {
				if (content.get(i).get(keyList.get(j)) == null) {
					sqlBuf.append("null,");
				} else if (content.get(i).get(keyList.get(j)).equals("null")) {
					sqlBuf.append("null,");
				} else if (content.get(i).get(keyList.get(j)).equals("now()")) {
					sqlBuf.append("now(),");
				} else {
					sqlBuf.append("'");
					sqlBuf.append(content.get(i).get(keyList.get(j)).toString().replaceAll("'", "''"));
					sqlBuf.append("',");
				}
			}
			sqlBuf.deleteCharAt(sqlBuf.length() - 1);
			sqlBuf.append("),");
		}
		sqlBuf.deleteCharAt(sqlBuf.length() - 1);
		sqlBuf.append("ON CONFLICT ON CONSTRAINT " + constraint + " DO update set ");
		for (int i = 0; i < keyList.size(); i++) {
			sqlBuf.append(DOUBLEMARKS + keyList.get(i) + DOUBLEMARKS + "=EXCLUDED." + keyList.get(i) + Symbol.COMMA);
		}
		sqlBuf.deleteCharAt(sqlBuf.length() - 1);
		return sqlBuf.toString();
	}

	/**
	 * 导出csv
	 */
	public String exportCsv(String sql) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			StringBuffer sb = new StringBuffer();
			connection = datasource.getConnection();
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
			LOGGER.error(Symbol.BLANK, e);
			return "";
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ps != null) {
					ps.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
				LOGGER.error(Symbol.BLANK, e);
			}
		}
	}

	/**
	 * 导入csv，第一行为字段名
	 */
	public boolean importCsv(String tableName, String content, boolean isReplace) {
		String[] contentArray = content.split("\r\n");
		if (contentArray.length < 2) {
			return false;
		}
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
		if (executeWithId(sqlBuf.toString()) == null) {
			return false;
		}
		return true;
	}

	private static String replaceNull(String str) {
		if (str == null) {
			return "null";
		} else {
			return str;
		}
	}

}
