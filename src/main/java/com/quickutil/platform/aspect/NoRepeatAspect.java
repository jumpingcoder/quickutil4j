package com.quickutil.platform.aspect;

import com.quickutil.platform.EnvironmentUtil;
import com.quickutil.platform.exception.NoRepeatException;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

@Aspect
@Component
public class NoRepeatAspect implements Ordered {

	private static final Logger LOGGER = LoggerFactory.getLogger(NoRepeatAspect.class);
	private static JedisPool jedisPool = null;

	public static void setJedisPool(JedisPool pool) {
		jedisPool = pool;
	}

	@Override
	public int getOrder() {
		return 1;
	}

	@Pointcut("@annotation(notRepeat)")
	public void pointCut(NoRepeat notRepeat) {
	}

	@Around("pointCut(notRepeat)")
	public Object around(ProceedingJoinPoint point, NoRepeat notRepeat) throws Throwable {
		LOGGER.debug("NoRepeat around");
		if (jedisPool == null) {
			throw new NullPointerException("NotRepeat need set jedis pool before use");
		}
		String keyName = String.format("nopeat:%s:%s", point.getSignature().getDeclaringTypeName(), point.getSignature().getName());
		String value = EnvironmentUtil.getMachineFinger() + ":" + System.currentTimeMillis();
		Jedis jedis = jedisPool.getResource();
		try {
			String result = jedis.get(keyName);
			result = jedis.set(keyName, value, new SetParams().nx().ex(notRepeat.lockExpire()));
			if (!"OK".equals(result)) {
				throw new NoRepeatException(keyName + " is running");
			}
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
		return point.proceed();
	}

	@Before("pointCut(notRepeat)")
	public void before(NoRepeat notRepeat) {
		LOGGER.debug("NoRepeat before");
	}

	@After("pointCut(notRepeat)")
	public void After(NoRepeat notRepeat) {
		LOGGER.debug("NoRepeat after");
	}

}
