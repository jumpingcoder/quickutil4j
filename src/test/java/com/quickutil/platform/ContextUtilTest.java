package com.quickutil.platform;

import org.junit.Assert;
import org.junit.Test;


/**
 * ContextUtilTest
 *
 * @author 0.5
 */

public class ContextUtilTest {

	@Test
	public void getOriginalUrl() {
		Assert.assertEquals("https://abc:123@quickutil.com:443/hello?xx=123#part1", ContextUtil.getOriginalUrl("http://abc:123@vndjsak.com:443/hello?xx=123#part1", "https", "quickutil.com"));
		Assert.assertEquals("https://quickutil.com/#part1", ContextUtil.getOriginalUrl("http://vndjsak.com/#part1", "https", "quickutil.com"));
		Assert.assertEquals("https://quickutil.com", ContextUtil.getOriginalUrl("http://vndjsak.com", "https", "quickutil.com"));
	}
}