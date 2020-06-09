package com.quickutil.platform.def;

import com.quickutil.platform.JedisUtil;

/**
 * CronJob
 *
 * CronJobUtil的实体类，用于保存任务属性
 *
 * @author 0.5
 */

public class CronJob {

	private String jobName;
	private String classpath;
	private String cron;
	private String params;
	private boolean available;
	private boolean lock;
	private int lockExpire;
	private JedisUtil jedisUtil;

	public CronJob(String jobName, String classpath, String cron, String params, boolean available, boolean lock, int lockExpire, JedisUtil jedisUtil) {
		this.jobName = jobName;
		this.classpath = classpath;
		this.cron = cron;
		this.params = params;
		this.available = available;
		this.lock = lock;
		this.lockExpire = lockExpire;
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

	public boolean getLock() {
		return this.lock;
	}

	public JedisUtil getJedisUtil() {
		return this.jedisUtil;
	}

	public int getLockExpire() {
		return this.lockExpire;
	}

}
