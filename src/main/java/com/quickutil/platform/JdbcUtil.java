package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.alibaba.druid.pool.DruidDataSource;
import com.quickutil.platform.constants.Symbol;
import com.quickutil.platform.entity.ResultSetDef;
import org.slf4j.LoggerFactory;

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
	private static final String COMMA = ",";
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


	private DruidDataSource buildDataSource(String dbName, String jdbcUrl, String username, String password, int initconnum, int minconnum, int maxconnum, Properties pool) {
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
			datasource.setMaxActive(maxconnum);
			if (pool == null) {
				return datasource;
			}
			if (pool.getProperty("DriverClass") != null) {
				datasource.setDriverClassName((pool.getProperty("DriverClass")));
			}
			if (pool.getProperty("AccessToUnderlyingConnectionAllowed") != null) {
				datasource.setAccessToUnderlyingConnectionAllowed(Boolean.parseBoolean(pool.getProperty("AccessToUnderlyingConnectionAllowed")));
			}
			if (pool.getProperty("AsyncCloseConnectionEnable") != null) {
				datasource.setAsyncCloseConnectionEnable(Boolean.parseBoolean(pool.getProperty("AsyncCloseConnectionEnable")));
			}
			if (pool.getProperty("BreakAfterAcquireFailure") != null) {
				datasource.setBreakAfterAcquireFailure(Boolean.parseBoolean(pool.getProperty("BreakAfterAcquireFailure")));
			}
			if (pool.getProperty("ClearFiltersEnable") != null) {
				datasource.setClearFiltersEnable(Boolean.parseBoolean(pool.getProperty("ClearFiltersEnable")));
			}
			if (pool.getProperty("ConnectionErrorRetryAttempts") != null) {
				datasource.setConnectionErrorRetryAttempts(Integer.parseInt(pool.getProperty("ConnectionErrorRetryAttempts")));
			}
			if (pool.getProperty("DbType") != null) {
				datasource.setDbType(pool.getProperty("DbType"));
			}
			if (pool.getProperty("DefaultAutoCommit") != null) {
				datasource.setDefaultAutoCommit(Boolean.parseBoolean(pool.getProperty("DefaultAutoCommit")));
			}
			if (pool.getProperty("DefaultCatalog") != null) {
				datasource.setDefaultCatalog(pool.getProperty("DefaultCatalog"));
			}
			if (pool.getProperty("DefaultReadOnly") != null) {
				datasource.setDefaultReadOnly(Boolean.parseBoolean(pool.getProperty("DefaultReadOnly")));
			}
			if (pool.getProperty("DefaultTransactionIsolation") != null) {
				datasource.setDefaultTransactionIsolation(Integer.parseInt(pool.getProperty("DefaultTransactionIsolation")));
			}
			if (pool.getProperty("Enable") != null) {
				datasource.setEnable(Boolean.parseBoolean(pool.getProperty("Enable")));
			}
			if (pool.getProperty("FailFast") != null) {
				datasource.setFailFast(Boolean.parseBoolean(pool.getProperty("FailFast")));
			}
			if (pool.getProperty("InitGlobalVariants") != null) {
				datasource.setInitGlobalVariants(Boolean.parseBoolean(pool.getProperty("InitGlobalVariants")));
			}
			if (pool.getProperty("InitVariants") != null) {
				datasource.setInitVariants(Boolean.parseBoolean(pool.getProperty("InitVariants")));
			}
			if (pool.getProperty("KeepAlive") != null) {
				datasource.setKeepAlive(Boolean.parseBoolean(pool.getProperty("KeepAlive")));
			}
			if (pool.getProperty("KillWhenSocketReadTimeout") != null) {
				datasource.setKillWhenSocketReadTimeout(Boolean.parseBoolean(pool.getProperty("KillWhenSocketReadTimeout")));
			}
			if (pool.getProperty("LogAbandoned") != null) {
				datasource.setLogAbandoned(Boolean.parseBoolean(pool.getProperty("LogAbandoned")));
			}
			if (pool.getProperty("LogDifferentThread") != null) {
				datasource.setLogDifferentThread(Boolean.parseBoolean(pool.getProperty("LogDifferentThread")));
			}
			if (pool.getProperty("LoginTimeout") != null) {
				datasource.setLoginTimeout(Integer.parseInt(pool.getProperty("LoginTimeout")));
			}
			if (pool.getProperty("MaxCreateTaskCount") != null) {
				datasource.setMaxCreateTaskCount(Integer.parseInt(pool.getProperty("MaxCreateTaskCount")));
			}
			if (pool.getProperty("MaxEvictableIdleTimeMillis") != null) {
				datasource.setMaxEvictableIdleTimeMillis(Long.parseLong(pool.getProperty("MaxEvictableIdleTimeMillis")));
			}
			if (pool.getProperty("MaxOpenPreparedStatements") != null) {
				datasource.setMaxOpenPreparedStatements(Integer.parseInt(pool.getProperty("MaxOpenPreparedStatements")));
			}
			if (pool.getProperty("PoolPreparedStatementPerConnectionSize") != null) {
				datasource.setMaxPoolPreparedStatementPerConnectionSize(Integer.parseInt(pool.getProperty("PoolPreparedStatementPerConnectionSize")));
			}
			if (pool.getProperty("MaxWait") != null) {
				datasource.setMaxWait(Long.parseLong(pool.getProperty("MaxWait")));
			}
			if (pool.getProperty("MaxWaitThreadCount") != null) {
				datasource.setMaxWaitThreadCount(Integer.parseInt(pool.getProperty("MaxWaitThreadCount")));
			}
			if (pool.getProperty("MinEvictableIdleTimeMillis") != null) {
				datasource.setMinEvictableIdleTimeMillis(Long.parseLong(pool.getProperty("MinEvictableIdleTimeMillis")));
			}
			if (pool.getProperty("MinIdle") != null) {
				datasource.setMinIdle(Integer.parseInt(pool.getProperty("MinIdle")));
			}
			if (pool.getProperty("NotFullTimeoutRetryCount") != null) {
				datasource.setNotFullTimeoutRetryCount(Integer.parseInt(pool.getProperty("NotFullTimeoutRetryCount")));
			}
			if (pool.getProperty("OnFatalErrorMaxActive") != null) {
				datasource.setOnFatalErrorMaxActive(Integer.parseInt(pool.getProperty("OnFatalErrorMaxActive")));
			}
			if (pool.getProperty("Oracle") != null) {
				datasource.setOracle(Boolean.parseBoolean(pool.getProperty("Oracle")));
			}
			if (pool.getProperty("PoolPreparedStatements") != null) {
				datasource.setPoolPreparedStatements(Boolean.parseBoolean(pool.getProperty("PoolPreparedStatements")));
			}
			if (pool.getProperty("QueryTimeout") != null) {
				datasource.setQueryTimeout(Integer.parseInt(pool.getProperty("QueryTimeout")));
			}
			if (pool.getProperty("RemoveAbandoned") != null) {
				datasource.setRemoveAbandoned(Boolean.parseBoolean(pool.getProperty("RemoveAbandoned")));
			}
			if (pool.getProperty("RemoveAbandonedTimeout") != null) {
				datasource.setRemoveAbandonedTimeout(Integer.parseInt(pool.getProperty("RemoveAbandonedTimeout")));
			}
			if (pool.getProperty("RemoveAbandonedTimeoutMillis") != null) {
				datasource.setRemoveAbandonedTimeoutMillis(Long.parseLong(pool.getProperty("RemoveAbandonedTimeoutMillis")));
			}
			if (pool.getProperty("ResetStatEnable") != null) {
				datasource.setResetStatEnable(Boolean.parseBoolean(pool.getProperty("ResetStatEnable")));
			}
			if (pool.getProperty("SharePreparedStatements") != null) {
				datasource.setSharePreparedStatements(Boolean.parseBoolean(pool.getProperty("SharePreparedStatements")));
			}
			if (pool.getProperty("TestOnBorrow") != null) {
				datasource.setTestOnBorrow(Boolean.parseBoolean(pool.getProperty("TestOnBorrow")));
			}
			if (pool.getProperty("TestOnReturn") != null) {
				datasource.setTestOnReturn(Boolean.parseBoolean(pool.getProperty("TestOnReturn")));
			}
			if (pool.getProperty("TimeBetweenConnectErrorMillis") != null) {
				datasource.setTimeBetweenConnectErrorMillis(Long.parseLong(pool.getProperty("TimeBetweenConnectErrorMillis")));
			}
			if (pool.getProperty("TimeBetweenEvictionRunsMillis") != null) {
				datasource.setTimeBetweenEvictionRunsMillis(Long.parseLong(pool.getProperty("TimeBetweenEvictionRunsMillis")));
			}
			if (pool.getProperty("TimeBetweenLogStatsMillis") != null) {
				datasource.setTimeBetweenLogStatsMillis(Long.parseLong(pool.getProperty("TimeBetweenLogStatsMillis")));
			}
			if (pool.getProperty("TransactionQueryTimeout") != null) {
				datasource.setTransactionQueryTimeout(Integer.parseInt(pool.getProperty("TransactionQueryTimeout")));
			}
			if (pool.getProperty("TransactionThresholdMillis") != null) {
				datasource.setTransactionThresholdMillis(Long.parseLong(pool.getProperty("TransactionThresholdMillis")));
			}
			if (pool.getProperty("UseGlobalDataSourceStat") != null) {
				datasource.setUseGlobalDataSourceStat(Boolean.parseBoolean(pool.getProperty("UseGlobalDataSourceStat")));
			}
			if (pool.getProperty("UseLocalSessionState") != null) {
				datasource.setUseLocalSessionState(Boolean.parseBoolean(pool.getProperty("UseLocalSessionState")));
			}
			if (pool.getProperty("UseOracleImplicitCache") != null) {
				datasource.setUseOracleImplicitCache(Boolean.parseBoolean(pool.getProperty("UseOracleImplicitCache")));
			}
			if (pool.getProperty("UseUnfairLock") != null) {
				datasource.setUseUnfairLock(Boolean.parseBoolean(pool.getProperty("UseUnfairLock")));
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
	public List<Object> getListObject(String sql) {
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
	 * 获取单列数据
	 */
	public List<Object> getListString(String sql) {
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
	 * 获取多列数据，有错误抛出
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
	 * 获取多列数据
	 */
	public List<Map<String, Object>> getListMap(String sql) {
		List<Map<String, Object>> list = new ArrayList<>();
		try {
			list = getListMapThrowable(sql);
		} catch (Exception e) {
			LOGGER.error(Symbol.BLANK, e);
		}
		return list;
	}

	/**
	 * 执行多条语句（前几条事务，最后一条查询）
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
	 * 获取ResultSet
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
		boolean result = execute(sql);
		if (!result) {
			LOGGER.debug(sql);
		}
		return result;
	}

	/**
	 * 批量替换数据-只适用于MySQL
	 */
	public boolean replaceListMap(String tableName, List<Map<String, Object>> content) {
		if (content.size() == 0) {
			return true;
		}
		String sql = combineInsert(tableName, content, true);
		boolean result = execute(sql);
		if (!result) {
			LOGGER.debug(sql);
		}
		return result;
	}

	/**
	 * 批量插入数据并返回最后一条自增id
	 */
	public Integer insertListMapWithId(String tableName, List<Map<String, Object>> content) {
		String sql = combineInsert(tableName, content, false);
		return executeWithId(sql);
	}

	/**
	 * 批量替换数据并返回最后一条自增id-只适用于MySQL
	 */
	public Integer replaceListMapWithId(String tableName, List<Map<String, Object>> content) {
		String sql = combineInsert(tableName, content, true);
		return executeWithId(sql);
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

	/**
	 * 批量upsert数据-适用于Postgre
	 */
	public boolean upsertListMap(String tableName, List<Map<String, Object>> content, String constraint) {
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
			sqlBuf.append(COMMA);
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
			sqlBuf.append(DOUBLEMARKS + keyList.get(i) + DOUBLEMARKS + "=EXCLUDED." + keyList.get(i) + COMMA);
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
