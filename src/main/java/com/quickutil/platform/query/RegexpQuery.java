package com.quickutil.platform.query;

import com.google.gson.JsonObject;
import com.quickutil.platform.StringUtil;
import com.quickutil.platform.exception.FormatQueryException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author shijie.ruan
 */
public class RegexpQuery extends QueryDSL {

	private String field, value;
	private List<FLAG> flagList = new LinkedList<>();
	private Integer maxDeterminizedStates;

	public RegexpQuery(String fieldName, String value) {
		super("regexp");
		this.field = fieldName;
		this.value = value;
	}

	public RegexpQuery setMaxDeterminizedStates(int maxDeterminizedStates) {
		this.maxDeterminizedStates = maxDeterminizedStates;
		return this;
	}

	public RegexpQuery addFlag(FLAG flag) {
		flagList.add(flag);
		return this;
	}

	public RegexpQuery setBoost(double boost) {
		this.boost = boost;
		return this;
	}

	@Override
	public JsonObject toJson() throws FormatQueryException {
		JsonObject regexpQuery = new JsonObject();
		if (null != boost || null != maxDeterminizedStates || !flagList.isEmpty()) {
			JsonObject fieldObject = new JsonObject();
			fieldObject.addProperty("value", value);
			if (null != boost) {
				fieldObject.addProperty("boost", boost);
			}
			if (null != maxDeterminizedStates) {
				fieldObject.addProperty("max_determinized_states", maxDeterminizedStates);
			}
			if (!flagList.isEmpty()) {
				List<String> flags = flagList.stream().map(Enum::toString).collect(Collectors.toList());
				String flagsString = StringUtil.joinString(flags.toArray(new String[0]), "|", "", "");
				fieldObject.addProperty("flags", flagsString);
			}
			regexpQuery.add(field, fieldObject);
		} else {
			regexpQuery.addProperty(field, value);
		}
		JsonObject queryDSL = new JsonObject();
		queryDSL.add(type, regexpQuery);
		return queryDSL;
	}

	enum FLAG {
		ALL, ANYSTRING, COMPLEMENT, EMPTY, INTERSECTION, INTERVAL, NONE
	}
}
