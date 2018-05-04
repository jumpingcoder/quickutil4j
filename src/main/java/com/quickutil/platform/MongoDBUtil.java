/**
 * MongoDB工具
 * 
 * @class MongoDBUtil
 * @author 0.5
 */
package com.quickutil.platform;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;

public class MongoDBUtil {

	private static final String mongoProtocol = "mongodb://%s:%s@%s:%s/%s";
	public static Map<String, MongoClient> mongoClientMap = new HashMap<String, MongoClient>();
	private static Map<String, String> mongoDBMap = new HashMap<String, String>();

	/**
	 * 增加MongoDB，默认pool配置
	 * 
	 * @param mongo-mongo配置
	 * @return
	 */
	public static boolean addMongoDB(Properties mongo) {
		return addMongoDB(mongo, null);
	}

	/**
	 * 增加MongoDB，指定pool配置
	 * 
	 * @param mongo-mongo配置
	 * @param pool-pool配置
	 * @return
	 */
	public static boolean addMongoDB(Properties mongo, Properties pool) {
		if (mongo == null)
			return false;
		Enumeration<?> keys = mongo.propertyNames();
		List<String> keyList = new ArrayList<String>();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			key = key.split("\\.")[0];
			if (!keyList.contains(key)) {
				keyList.add(key);
			}
		}
		for (String key : keyList) {
			String username = mongo.getProperty(key + ".username");
			String password = mongo.getProperty(key + ".password");
			String host = mongo.getProperty(key + ".host");
			int port = Integer.parseInt(mongo.getProperty(key + ".port"));
			String database = mongo.getProperty(key + ".database");
			try {
				MongoClient client = buildMongoDB(username, password, host, port, database, pool);
				mongoClientMap.put(key, client);
				mongoDBMap.put(key, database);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return true;
	}

	/**
	 * 增加MongoDB，默认pool配置
	 * 
	 * @param dbName-自定义数据库名
	 * @param username-用户名
	 * @param password-密码
	 * @param host-数据库host
	 * @param port-数据库端口
	 * @param database-数据库名
	 * @return
	 */
	public static boolean addMongoDB(String dbName, String username, String password, String host, int port, String database) {
		mongoClientMap.put(dbName, buildMongoDB(username, password, host, port, database, null));
		mongoDBMap.put(dbName, database);
		return true;
	}

	/**
	 * 增加MongoDB，指定pool配置
	 * 
	 * @param dbName-自定义数据库名
	 * @param username-用户名
	 * @param password-密码
	 * @param host-数据库host
	 * @param port-数据库端口
	 * @param database-数据库名
	 * @param pool-pool配置
	 * @return
	 */
	public static boolean addMongoDB(String dbName, String username, String password, String host, int port, String database, Properties pool) {
		mongoClientMap.put(dbName, buildMongoDB(username, password, host, port, database, pool));
		mongoDBMap.put(dbName, database);
		return true;
	}

	/**
	 * 获取生成的MongoDB
	 * 
	 * @param dbName-数据库名称
	 * @return
	 */
	public static MongoDatabase getMongoDB(String dbName) {
		return mongoClientMap.get(dbName).getDatabase(mongoDBMap.get(dbName));
	}

	/**
	 * 释放生成的MongoDB
	 * 
	 * @param dbName
	 */
	public void closeMongoDB(String dbName) {
		MongoClient client = mongoClientMap.get(dbName);
		if (client == null)
			return;
		client.close();
		mongoClientMap.remove(dbName);
		mongoDBMap.remove(dbName);
	}

	/**
	 * 释放所有生成的MongoDB
	 * 
	 */
	public void closeAllMongoDB() {
		for (String dbName : mongoClientMap.keySet()) {
			closeMongoDB(dbName);
		}
	}

	private static MongoClient buildMongoDB(String username, String password, String host, int port, String database, Properties pool) {
		try {
			Builder builder = new MongoClientOptions.Builder();
			if (pool != null) {
				if (pool.getProperty("ConnectTimeOut") != null)
					builder.connectTimeout(Integer.parseInt(pool.getProperty("ConnectTimeOut")));
				if (pool.getProperty("CursorFinalizerEnabled") != null)
					builder.cursorFinalizerEnabled(Boolean.parseBoolean(pool.getProperty("CursorFinalizerEnabled")));
				if (pool.getProperty("ConnectionsPerHost") != null)
					builder.connectionsPerHost(Integer.parseInt(pool.getProperty("ConnectionsPerHost")));
				if (pool.getProperty("HeartbeatConnectTimeout") != null)
					builder.heartbeatConnectTimeout(Integer.parseInt(pool.getProperty("HeartbeatConnectTimeout")));
				if (pool.getProperty("HeartbeatFrequency") != null)
					builder.heartbeatFrequency(Integer.parseInt(pool.getProperty("HeartbeatFrequency")));
				if (pool.getProperty("HeartbeatSocketTimeout") != null)
					builder.heartbeatSocketTimeout(Integer.parseInt(pool.getProperty("HeartbeatSocketTimeout")));
				if (pool.getProperty("LocalThreshold") != null)
					builder.localThreshold(Integer.parseInt(pool.getProperty("LocalThreshold")));
				if (pool.getProperty("MaxConnectionIdleTime") != null)
					builder.maxConnectionIdleTime(Integer.parseInt(pool.getProperty("MaxConnectionIdleTime")));
				if (pool.getProperty("MaxConnectionLifeTime") != null)
					builder.maxConnectionLifeTime(Integer.parseInt(pool.getProperty("MaxConnectionLifeTime")));
				if (pool.getProperty("MaxWaitTime") != null)
					builder.maxWaitTime(Integer.parseInt(pool.getProperty("MaxWaitTime")));
				if (pool.getProperty("MinConnectionsPerHost") != null)
					builder.minConnectionsPerHost(Integer.parseInt(pool.getProperty("MinConnectionsPerHost")));
				if (pool.getProperty("MinHeartbeatFrequency") != null)
					builder.minHeartbeatFrequency(Integer.parseInt(pool.getProperty("MinHeartbeatFrequency")));
				if (pool.getProperty("RequiredReplicaSetName") != null)
					builder.requiredReplicaSetName(pool.getProperty("RequiredReplicaSetName"));
				if (pool.getProperty("ServerSelectionTimeout") != null)
					builder.serverSelectionTimeout(Integer.parseInt(pool.getProperty("ServerSelectionTimeout")));
				builder.socketKeepAlive(true);
				if (pool.getProperty("SocketTimeout") != null)
					builder.socketTimeout(Integer.parseInt(pool.getProperty("SocketTimeout")));
				if (pool.getProperty("SslEnabled") != null)
					builder.sslEnabled(Boolean.parseBoolean(pool.getProperty("SslEnabled")));
				if (pool.getProperty("SslInvalidHostNameAllowed") != null)
					builder.sslInvalidHostNameAllowed(Boolean.parseBoolean(pool.getProperty("SslInvalidHostNameAllowed")));
				if (pool.getProperty("ThreadsAllowedToBlockForConnectionMultiplier") != null)
					builder.threadsAllowedToBlockForConnectionMultiplier(Integer.parseInt(pool.getProperty("ThreadsAllowedToBlockForConnectionMultiplier")));
			}
			MongoClient client = new MongoClient(new MongoClientURI(String.format(mongoProtocol, username, password, host, port, database), builder));
			return client;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 获取数据库列表
	 * 
	 * @param dbName-数据库名
	 * @return
	 */
	public static List<String> getDBNames(String dbName) {
		try {
			List<String> list = new ArrayList<String>();
			MongoCursor<String> cursor = mongoClientMap.get(dbName).listDatabaseNames().iterator();
			while (cursor.hasNext())
				list.add(cursor.next());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 获取数据库列表
	 * 
	 * @param dbName-数据库名
	 * @return
	 */
	public static List<String> getCollectionNames(String dbName) {
		MongoDatabase database = mongoClientMap.get(dbName).getDatabase(mongoDBMap.get(dbName));
		MongoCursor<String> cursor = database.listCollectionNames().iterator();
		List<String> list = new ArrayList<String>();
		while (cursor.hasNext())
			list.add(cursor.next());
		return list;
	}

	/**
	 * 创建表
	 * 
	 * @param dbName-数据库名
	 * @param collectionName-表名
	 * @return
	 */
	public static boolean createCollection(String dbName, String collectionName) {
		try {
			MongoDatabase database = mongoClientMap.get(dbName).getDatabase(mongoDBMap.get(dbName));
			database.createCollection(collectionName);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 创建索引
	 * 
	 * @param dbName-数据库名
	 * @param collectionName-表名
	 * @param index-索引格式
	 * @return
	 */
	public static String createIndex(String dbName, String collectionName, Bson index) {
		MongoDatabase database = mongoClientMap.get(dbName).getDatabase(mongoDBMap.get(dbName));
		return database.getCollection(collectionName).createIndex(index);
	}

	/**
	 * 查询数据
	 * 
	 * @param dbName-数据库名
	 * @param collectionName-表名
	 * @param filter-过滤条件
	 * @param sort-排序条件
	 * @param limit-查询数量
	 * @return
	 */
	public static List<Map<String, Object>> findListMap(String dbName, String collectionName, Bson filter, Bson sort, int limit) {
		MongoDatabase database = mongoClientMap.get(dbName).getDatabase(mongoDBMap.get(dbName));
		MongoCursor<Document> cursor = database.getCollection(collectionName).find(filter).sort(sort).limit(limit).iterator();
		List<Map<String, Object>> content = new ArrayList<Map<String, Object>>();
		while (cursor.hasNext()) {
			content.add(cursor.next());
		}
		return content;
	}

	/**
	 * 计算数量
	 * 
	 * @param dbName-数据库名
	 * @param collectionName-表名
	 * @param filter-过滤条件
	 * @return
	 */
	public static long countListMap(String dbName, String collectionName, Bson filter) {
		MongoDatabase database = mongoClientMap.get(dbName).getDatabase(mongoDBMap.get(dbName));
		return database.getCollection(collectionName).count(filter);
	}

	/**
	 * 写入数据
	 * 
	 * @param dbName-数据库名
	 * @param collectionName-表名
	 * @param content-写入的内容
	 * @return
	 */
	public static boolean insertListMap(String dbName, String collectionName, List<Map<String, Object>> content) {
		try {
			List<Document> list = new ArrayList<Document>();
			for (Map<String, Object> map : content) {
				list.add(new Document(map));
			}
			MongoDatabase database = mongoClientMap.get(dbName).getDatabase(mongoDBMap.get(dbName));
			database.getCollection(collectionName).insertMany(list);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 更新数据
	 * 
	 * @param dbName-数据库名
	 * @param collectionName-表名
	 * @param filter-过滤条件
	 * @param update-更新的内容
	 * @return
	 */
	public static UpdateResult updateListMap(String dbName, String collectionName, Bson filter, Bson update) {
		MongoDatabase database = mongoClientMap.get(dbName).getDatabase(mongoDBMap.get(dbName));
		return database.getCollection(collectionName).updateMany(filter, update);
	}

	/**
	 * 去重查找
	 * 
	 * @param dbName-数据库名
	 * @param collectionName-表名
	 * @param fieldName-字段名
	 * @param classType-字段类型
	 * @param filter-过滤条件
	 * @return
	 */
	public static <T, TResult> List<Object> distinct(String dbName, String collectionName, String fieldName, Class<TResult> classType, Bson filter) {
		MongoDatabase database = mongoClientMap.get(dbName).getDatabase(mongoDBMap.get(dbName));
		MongoCursor<TResult> cursor = database.getCollection(collectionName).distinct(fieldName, filter, classType).iterator();
		List<Object> content = new ArrayList<Object>();
		while (cursor.hasNext()) {
			content.add(cursor.next());
		}
		return content;
	}

	/**
	 * 聚合数据
	 * 
	 * @param dbName-数据库名
	 * @param collectionName-表名
	 * @param pipeline-聚合条件
	 * @return
	 */
	public static List<Map<String, Object>> aggregateListMap(String dbName, String collectionName, List<Bson> pipeline) {
		MongoDatabase database = mongoClientMap.get(dbName).getDatabase(mongoDBMap.get(dbName));
		MongoCursor<Document> cursor = database.getCollection(collectionName).aggregate(pipeline).iterator();
		List<Map<String, Object>> content = new ArrayList<Map<String, Object>>();
		while (cursor.hasNext()) {
			content.add(cursor.next());
		}
		return content;
	}

	private static final String FROM = " from ";
	private static final String UPDATE = "update ";
	private static final String SET = " set ";
	private static final String WHERE = " where ";
	private static final String AND = " and ";
	private static final String ORDER = " order by ";
	private static final String ASC = "asc";
	private static final String DESC = "desc";
	private static final String LIMIT = " limit ";
	private static final String LTE = "<=";
	private static final String LT = "<";
	private static final String GTE = ">=";
	private static final String GT = ">";
	private static final String NEQ = "!=";
	private static final String EQ = "=";
	private static final String COMMA = ",";
	private static final String BLANK = " ";

	/**
	 * 查询Mongo的SQL解析器
	 * 
	 * @param dbName-数据库名
	 * @param sql-查询语句
	 * @return
	 */
	public static List<Map<String, Object>> selectSql(String dbName, String sql) {
		List<Bson> filterList = new ArrayList<Bson>();
		List<Bson> sortList = new ArrayList<Bson>();
		sql = sql.replaceAll("\"", "");
		sql = sql.replaceAll("\'", "");
		int fromIndex = sql.indexOf(FROM);
		int whereIndex = sql.indexOf(WHERE);
		int orderIndex = sql.indexOf(ORDER);
		int limitIndex = sql.indexOf(LIMIT);
		int limit = 0;
		// tableName
		String tableName = null;
		if (fromIndex == -1)
			return null;
		tableName = sql.substring(fromIndex + 6);
		int index = tableName.indexOf(BLANK);
		if (index != -1)
			tableName = tableName.substring(0, index);
		// where
		if (whereIndex != -1) {
			String whereStr = null;
			if (orderIndex != -1)
				whereStr = sql.substring(whereIndex + 7, orderIndex);
			else if (limitIndex != -1)
				whereStr = sql.substring(whereIndex + 7, limitIndex);
			else
				whereStr = sql.substring(whereIndex + 7);
			String whereArray[] = whereStr.split(AND);
			for (String where : whereArray) {
				where = where.trim();
				String[] splitArray = where.split(LTE);
				if (splitArray.length == 2) {
					filterList.add(Filters.lte(splitArray[0], StringUtil.getObjectFromString(splitArray[1])));
					continue;
				}
				splitArray = where.split(LT);
				if (splitArray.length == 2) {
					filterList.add(Filters.lt(splitArray[0], StringUtil.getObjectFromString(splitArray[1])));
					continue;
				}
				splitArray = where.split(GTE);
				if (splitArray.length == 2) {
					filterList.add(Filters.gte(splitArray[0], StringUtil.getObjectFromString(splitArray[1])));
					continue;
				}
				splitArray = where.split(GT);
				if (splitArray.length == 2) {
					filterList.add(Filters.gt(splitArray[0], StringUtil.getObjectFromString(splitArray[1])));
					continue;
				}
				splitArray = where.split(NEQ);
				if (splitArray.length == 2) {
					filterList.add(Filters.ne(splitArray[0], StringUtil.getObjectFromString(splitArray[1])));
					continue;
				}
				splitArray = where.split(EQ);
				if (splitArray.length == 2) {
					filterList.add(Filters.eq(splitArray[0], StringUtil.getObjectFromString(splitArray[1])));
					continue;
				}
			}
		}
		// order
		if (orderIndex != -1) {
			String orderStr = null;
			if (limitIndex != -1)
				orderStr = sql.substring(orderIndex + 10, limitIndex);
			else
				orderStr = sql.substring(orderIndex + 10);
			String[] orderArray = orderStr.split(COMMA);
			for (String order : orderArray) {
				String[] splitArray = order.split(BLANK);
				if (splitArray.length != 2)
					continue;
				if (splitArray[1].equals(DESC)) {
					sortList.add(Sorts.descending(splitArray[0]));
				} else if (splitArray[1].equals(ASC)) {
					sortList.add(Sorts.ascending(splitArray[0]));
				}
			}
		}
		// limit
		if (limitIndex != -1) {
			String limitStr = sql.substring(limitIndex + 7);
			String[] splitArray = limitStr.split(COMMA);
			if (splitArray.length == 2) {
				int start = (int) StringUtil.getObjectFromString(splitArray[0]);
				int end = (int) StringUtil.getObjectFromString(splitArray[1]);
				limit = end - start;
			}
		}
		return findListMap(dbName, tableName, Filters.and(filterList), Sorts.orderBy(sortList), limit);
	}

	/**
	 * 更新sql解析器
	 * 
	 * @param dbName-数据库名
	 * @param sql-更新语句
	 * @return
	 */
	public static UpdateResult updateSql(String dbName, String sql) {
		List<Bson> filterList = new ArrayList<Bson>();
		List<Bson> updateList = new ArrayList<Bson>();
		sql = sql.replaceAll("\"", "");
		sql = sql.replaceAll("\'", "");
		int updateIndex = sql.indexOf(UPDATE);
		int setIndex = sql.indexOf(SET);
		int whereIndex = sql.indexOf(WHERE);
		// tableName
		String tableName = null;
		if (updateIndex == -1 || setIndex == -1)
			return null;
		tableName = sql.substring(updateIndex + 7);
		int index = tableName.indexOf(BLANK);
		tableName = tableName.substring(0, index);
		// set
		String setStr = null;
		if (whereIndex == -1) {
			setStr = sql.substring(setIndex + 5);
		} else {
			setStr = sql.substring(setIndex + 5, whereIndex);
		}
		String setArray[] = setStr.split(COMMA);
		for (String set : setArray) {
			set = set.trim();
			String[] splitArray = set.split(EQ);
			if (splitArray.length != 2)
				return null;
			updateList.add(Updates.set(splitArray[0], StringUtil.getObjectFromString(splitArray[1])));
		}
		// where
		if (whereIndex != -1) {
			String whereStr = sql.substring(whereIndex + 7);
			String whereArray[] = whereStr.split(AND);
			for (String where : whereArray) {
				where = where.trim();
				String[] splitArray = where.split(LTE);
				if (splitArray.length == 2) {
					filterList.add(Filters.lte(splitArray[0], StringUtil.getObjectFromString(splitArray[1])));
					continue;
				}
				splitArray = where.split(LT);
				if (splitArray.length == 2) {
					filterList.add(Filters.lt(splitArray[0], StringUtil.getObjectFromString(splitArray[1])));
					continue;
				}
				splitArray = where.split(GTE);
				if (splitArray.length == 2) {
					filterList.add(Filters.gte(splitArray[0], StringUtil.getObjectFromString(splitArray[1])));
					continue;
				}
				splitArray = where.split(GT);
				if (splitArray.length == 2) {
					filterList.add(Filters.gt(splitArray[0], StringUtil.getObjectFromString(splitArray[1])));
					continue;
				}
				splitArray = where.split(NEQ);
				if (splitArray.length == 2) {
					filterList.add(Filters.ne(splitArray[0], StringUtil.getObjectFromString(splitArray[1])));
					continue;
				}
				splitArray = where.split(EQ);
				if (splitArray.length == 2) {
					filterList.add(Filters.eq(splitArray[0], StringUtil.getObjectFromString(splitArray[1])));
					continue;
				}
			}
		}
		return updateListMap(dbName, tableName, Filters.and(filterList), Updates.combine(updateList));
	}
}
