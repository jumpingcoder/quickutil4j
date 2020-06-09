package com.quickutil.platform;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.quickutil.platform.ContextUtil;

/**
 * Http会话过滤器
 *
 * @author 0.5
 */
public class ContextFilter implements Filter {

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		ContextUtil.setRequest((HttpServletRequest) request);
		ContextUtil.setResponse((HttpServletResponse) response);
		chain.doFilter(request, response);
	}

	@Override
	public void destroy() {
	}

}
