package com.quickutil.platform.aspect;

import com.quickutil.platform.EnvironmentUtil;
import com.quickutil.platform.annotation.NoRepeat;
import com.quickutil.platform.exception.NoRepeatException;
import java.util.ArrayList;
import java.util.List;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.params.SetParams;

@Aspect
public class NoRepeatAspect implements Ordered {

	private static final Logger LOGGER = LoggerFactory.getLogger(NoRepeatAspect.class);
	private JedisPool jedisPool = null;

	public NoRepeatAspect(JedisPool jedisPool) {
		this.jedisPool = jedisPool;
	}

	@Override
	public int getOrder() {
		return 1;
	}

	@Pointcut("@annotation(noRepeat)")
	public void pointCut(NoRepeat noRepeat) {
	}

	@Around("pointCut(noRepeat)")
	public Object around(ProceedingJoinPoint point, NoRepeat noRepeat) throws Throwable {
		if (jedisPool == null) {
			throw new NullPointerException("NoRepeat need set jedis pool before use");
		}
		String keyName = String.format("nopeat:%s:%s", point.getSignature().getDeclaringTypeName(), point.getSignature().getName());
		String value = EnvironmentUtil.getMachineFinger() + ":" + System.currentTimeMillis();
		Jedis jedis = jedisPool.getResource();
		try {
			String result = jedis.set(keyName, value, new SetParams().nx().ex(noRepeat.lockExpire()));
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

	@After("pointCut(noRepeat)")
	public void After(JoinPoint point, NoRepeat noRepeat) {
		String keyName = String.format("nopeat:%s:%s", point.getSignature().getDeclaringTypeName(), point.getSignature().getName());
		Jedis jedis = jedisPool.getResource();
		try {
			jedis.del(keyName);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	public List<String> getLocks() {
		return getLocks("norepeat:*");
	}

	public List<String> getLock(String className, String functionName) {
		return getLocks(String.format("nopeat:%s:%s", className, functionName));
	}

	private List<String> getLocks(String pattern) {
		List<String> resultList = new ArrayList<>();
		String cursor = ScanParams.SCAN_POINTER_START;
		ScanParams params = new ScanParams().count(100).match(pattern);
		while (true) {
			Jedis jedis = jedisPool.getResource();
			try {
				ScanResult<String> result = jedis.scan(cursor, params);
				resultList.addAll(result.getResult());
				cursor = result.getCursor();
				if (cursor.equals("0")) {
					break;
				}
			} finally {
				jedis.close();
			}
		}
		return resultList;
	}
}
