package com.quickutil.platform.def;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.CronJobUtil;
import com.quickutil.platform.JedisUtil;
import java.lang.reflect.Method;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.LoggerFactory;

public class CronJob implements Job {

	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(CronJob.class);
	private String jobName;
	private String classpath;
	private String method;
	private String cron;
	private String params;
	private boolean available;
	private boolean repeat;
	private JedisUtil jedisUtil;
	private boolean loaded;

	public CronJob() {
	}

	public CronJob(String jobName, String classpath, String method, String cron, String params, boolean available, boolean repeat, JedisUtil jedisUtil) {
		this.jobName = jobName;
		this.classpath = classpath;
		this.method = method;
		this.cron = cron;
		this.params = params;
		this.available = available;
		this.repeat = repeat;
		this.jedisUtil = jedisUtil;
	}

	public String getJobName() {
		return this.jobName;
	}

	public String getClasspath() {
		return this.classpath;
	}

	public String getCron() {
		return this.cron;
	}

	public String getParams() {
		return this.params;
	}

	public boolean getAvailable() {
		return this.available;
	}

	public boolean getRepeat() {
		return this.repeat;
	}

	public JedisUtil getJedisUtil() {
		return this.jedisUtil;
	}

	public boolean getLoaded() {
		return this.loaded;
	}

	public void setLoaded(boolean loaded) {
		this.loaded = loaded;
	}

	public String getMethod() {
		return this.method;
	}

	@Override
	public void execute(JobExecutionContext context) {
		try {
			CronJob cronJob = CronJobUtil.getJob(context.getJobDetail().getKey().getName());
			Class c = Class.forName(cronJob.getClasspath());
			Method m = c.getDeclaredMethod(cronJob.getMethod(), CronJob.class);
			LOGGER.info("Job " + cronJob.getJobName() + " start");
			m.invoke(c.newInstance(), cronJob);
			LOGGER.info("Job " + cronJob.getJobName() + " finished");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
