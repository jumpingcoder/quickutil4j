package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.def.CronJob;
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
					boolean repeat = Boolean.parseBoolean(jobProperties.getProperty(jobName + ".repeat"));
					CronJob cronJob = new CronJob(jobName, classpath, method, cron, params, available, repeat, jedisUtil);
					startJob(cronJob);
				} catch (Exception e) {
					LOGGER.error("Job " + jobName + " properties load failed", e);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static CronJob getJob(String jobName) {
		return jobNameToJob.get(jobName);
	}

	//if you want deploy two or more process, you should use this function together
	public static void startJob(CronJob cronJob) {
		try {
			if (sd.checkExists(new JobKey(cronJob.getJobName()))) {
				LOGGER.warn("Job " + cronJob.getJobName() + " load failed, " + cronJob.getJobName() + " already exists");
			}
			jobNameToJob.put(cronJob.getJobName(), cronJob);
			if (null == cronJob.getJobName() || null == cronJob.getClasspath() || null == cronJob.getCron()) {
				LOGGER.warn("Job " + cronJob.getJobName() + " load failed, jobName, classpath, cron can not be null");
				return;
			}
			if (!cronJob.getRepeat() && null == cronJob.getJedisUtil()) {
				LOGGER.warn("Job " + cronJob.getJobName() + " load failed, can not run with repeat=false and jedisutil=null");
				return;
			}
			jobNameToJob.put(cronJob.getJobName(), cronJob);
			if (!cronJob.getAvailable()) {
				LOGGER.info("Job" + cronJob.getJobName() + " available is false, will not be loaded");
				return;
			}
			JobDetail jd = JobBuilder.newJob((Class<? extends Job>) Class.forName("com.quickutil.platform.def.CronJob")).withIdentity(cronJob.getJobName()).build();
			Trigger tg = TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule(cronJob.getCron())).build();
			sd.scheduleJob(jd, tg);
			LOGGER.info("Job " + cronJob.getJobName() + " load successfully");
		} catch (ClassNotFoundException e) {
			LOGGER.warn("Job " + cronJob.getJobName() + " load failed, classpath is wrong");
		} catch (RuntimeException e) {
			LOGGER.warn("Job " + cronJob.getJobName() + " load failed, cron is wrong");
		} catch (Exception e) {
			LOGGER.error("Job " + cronJob.getJobName() + " load failed, exception", e);
		}
	}


}
