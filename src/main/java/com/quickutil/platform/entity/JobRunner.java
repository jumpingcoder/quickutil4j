package com.quickutil.platform.entity;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.CronJobUtil;
import com.quickutil.platform.IPUtil;
import java.lang.reflect.Method;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.LoggerFactory;

/**
 * JobRunner
 *
 * 用于Quartz和真正Job间的任务组装，包括日志、lock机制等
 *
 * @author 0.5
 */

public class JobRunner implements Job {

	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(JobRunner.class);

	public JobRunner() {
	}

	@Override
	public void execute(JobExecutionContext context) {
		CronJob cronJob = CronJobUtil.getJob(context.getJobDetail().getKey().getName());
		try {
			//执行前判断是否有锁
			if (cronJob.getLock()) {
				boolean lockSuccess = cronJob.getJedisUtil().setnxWithExpire("CronJob::" + cronJob.getJobName(), IPUtil.getIpv4ListString(), cronJob.getLockExpire());
				if (!lockSuccess) {
					LOGGER.info("CronJob " + cronJob.getJobName() + " get lock failed, other process is running");
					return;
				}
			}
			Class c = Class.forName(cronJob.getClasspath());
			Method m = c.getDeclaredMethod("run", CronJob.class);
			LOGGER.info("CronJob " + cronJob.getJobName() + " start");
			m.invoke(c.newInstance(), cronJob);
			//执行完毕移除锁
			if (cronJob.getLock()) {
				cronJob.getJedisUtil().deleteKey("CronJob::" + cronJob.getJobName());
			}
			LOGGER.info("CronJob " + cronJob.getJobName() + " finished");
		} catch (ClassNotFoundException e1) {
			LOGGER.error("CronJob " + cronJob.getJobName() + " classpath is wrong");
		} catch (NoSuchMethodException e2) {
			LOGGER.error("CronJob " + cronJob.getJobName() + " class should have method run(CronJob c)");
		} catch (Exception e3) {
			LOGGER.error("", e3);
		}
	}

}
