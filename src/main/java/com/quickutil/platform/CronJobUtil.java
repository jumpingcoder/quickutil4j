package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.constants.Symbol;
import com.quickutil.platform.entity.CronJob;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.LoggerFactory;

/**
 * CronJobUtil
 *
 * 基于Quartz实现的cronjob工具类，支持redis实现锁，统一了任务执行前后的日志结构
 *
 * @author 0.5
 */

public class CronJobUtil {

	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(CronJobUtil.class);
	private static Scheduler sd = null;
	private static Map<String, CronJob> jobNameToJob = new HashMap<>();

	public static void init(Properties jobProperties, JedisUtil jedisUtil) {
		try {
			sd = new StdSchedulerFactory().getScheduler();
			sd.start();
			Enumeration<?> keys = jobProperties.propertyNames();
			List<String> keyList = new ArrayList<String>();
			while (keys.hasMoreElements()) {
				String key = (String) keys.nextElement();
				key = key.split("\\.")[0];
				if (!keyList.contains(key)) {
					keyList.add(key);
				}
			}
			for (String jobName : keyList) {
				try {
					String classpath = jobProperties.getProperty(jobName + ".classpath");
					String method = jobProperties.getProperty(jobName + ".method");
					String cron = jobProperties.getProperty(jobName + ".cron");
					String params = jobProperties.getProperty(jobName + ".params");
					boolean available = Boolean.parseBoolean(jobProperties.getProperty(jobName + ".available"));
					boolean lock = Boolean.parseBoolean(jobProperties.getProperty(jobName + ".lock"));
					int lockExpire = Integer.parseInt(jobProperties.getProperty(jobName + ".lockExpire"));
					CronJob cronJob = new CronJob(jobName, classpath, cron, params, available, lock, lockExpire, jedisUtil);
					startJob(cronJob);
				} catch (Exception e) {
					LOGGER.error("CronJob " + jobName + " properties load failed", e);
				}
			}
		} catch (Exception e) {
			LOGGER.error(Symbol.BLANK,e);
		}
	}

	public static CronJob getJob(String jobName) {
		return jobNameToJob.get(jobName);
	}

	//if you want deploy two or more process, you should use this function together
	public static void startJob(CronJob cronJob) {
		try {
			if (sd.checkExists(new JobKey(cronJob.getJobName()))) {
				LOGGER.warn("CronJob " + cronJob.getJobName() + " load failed, " + cronJob.getJobName() + " already exists");
			}
			if (null == cronJob.getJobName() || null == cronJob.getClasspath() || null == cronJob.getCron()) {
				LOGGER.warn("CronJob " + cronJob.getJobName() + " load failed, jobName, classpath, cron can not be null");
				return;
			}
			if (cronJob.getLock() && null == cronJob.getJedisUtil()) {
				LOGGER.warn("CronJob " + cronJob.getJobName() + " load failed, when lock=true, jedisutil cannot be null ");
				return;
			}
			jobNameToJob.put(cronJob.getJobName(), cronJob);
			if (!cronJob.getAvailable()) {
				LOGGER.info("CronJob" + cronJob.getJobName() + " available is false, will not be loaded");
				return;
			}
			JobDetail jd = JobBuilder.newJob((Class<? extends Job>) Class.forName("com.quickutil.platform.entity.JobRunner")).withIdentity(cronJob.getJobName()).build();
			Trigger tg = TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule(cronJob.getCron())).build();
			sd.scheduleJob(jd, tg);
			LOGGER.info("CronJob " + cronJob.getJobName() + " load successfully");
		} catch (ClassNotFoundException e) {
			LOGGER.warn("CronJob " + cronJob.getJobName() + " load failed, classpath is wrong");
		} catch (RuntimeException e) {
			LOGGER.warn("CronJob " + cronJob.getJobName() + " load failed, cron is wrong");
		} catch (Exception e) {
			LOGGER.error("CronJob " + cronJob.getJobName() + " load failed, exception", e);
		}
	}

	public static void clear() {
		try {
			sd.clear();
			jobNameToJob.clear();
		} catch (Exception e) {
			LOGGER.error("CronJob clear failed", e);
		}
	}

}
