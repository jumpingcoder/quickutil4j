/**
 * JdbcPoolUtil
 *
 * @class JdbcPoolUtil
 * @author 0.5
 */

package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JdbcPoolUtil {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(JdbcPoolUtil.class);
    private Map<String, JdbcUtil> jdbcUtilMap = new HashMap<>();

    public JdbcPoolUtil() {

    }

    public JdbcPoolUtil(Properties jdbcProperties, Properties druidProperties) {
        Enumeration<?> keys = jdbcProperties.propertyNames();
        List<String> keyList = new ArrayList<String>();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            key = key.split("\\.")[0];
            if (!keyList.contains(key)) {
                keyList.add(key);
            }
        }
        for (String dbName : keyList) {
            try {
                String url = jdbcProperties.getProperty(dbName + ".url");
                String username = jdbcProperties.getProperty(dbName + ".username");
                String password = jdbcProperties.getProperty(dbName + ".password");
                int initconnum = Integer.parseInt(jdbcProperties.getProperty(dbName + ".initconnum"));
                int minconnum = Integer.parseInt(jdbcProperties.getProperty(dbName + ".minconnum"));
                int maxconnum = Integer.parseInt(jdbcProperties.getProperty(dbName + ".maxconnum"));
                jdbcUtilMap.put(dbName, new JdbcUtil(dbName, url, username, password, initconnum, minconnum, maxconnum, druidProperties));
            } catch (Exception e) {
                LOGGER.error("", e);
            }
        }
    }


    public void add(String dbName, String url, String username, String password, int initconnum, int minconnum, int maxconnum, Properties druidProperties) {
        jdbcUtilMap.put(dbName, new JdbcUtil(dbName, url, username, password, initconnum, minconnum, maxconnum, druidProperties));
    }

    public JdbcUtil get(String dbName) {
        return jdbcUtilMap.get(dbName);
    }

    public void remove(String dbName) {
        jdbcUtilMap.get(dbName).closeDataSource();
        jdbcUtilMap.remove(dbName);
    }
}
