package com.quickutil.platform;

import com.quickutil.platform.aggs.AggsDSL;
import com.quickutil.platform.query.QueryDSL;

/**
 * @author shijie.ruan
 */
public class Query {
	private int size;
	private int from;

	private QueryDSL query;
	private AggsDSL aggs;
}
