package com.quickutil.platform.query;

import com.google.gson.JsonObject;
import com.quickutil.platform.FormatQueryException;

/**
 * 现在仅支持使用文件形式的 groovy 脚本, 因为它是2.x 和5.x 都支持,而且不用装插件的脚本
 * @author shijie.ruan
 */
public class ScriptQuery extends QueryDSL {
	private String scriptFile = null;
	private JsonObject params = null;

	public ScriptQuery(String scriptFile) {
		super("script");
		this.scriptFile = scriptFile;
	}

	public ScriptQuery(String scriptFile, JsonObject params) {
		super("script");
		this.scriptFile = scriptFile;
		this.params = params;
	}

	public ScriptQuery setScript(String scriptFile) {
		this.scriptFile = scriptFile;
		return this;
	}

	public ScriptQuery setParams(JsonObject params) {
		this.params = params;
		return this;
	}

	public ScriptQuery setBoost(double boost) {
		this.boost = boost;
		return this;
	}

	@Override
	public JsonObject toJson() throws FormatQueryException {
		JsonObject scriptObject1 = new JsonObject(); // query api 中使用的脚本需要两层 script 包装
		JsonObject scriptObject = new JsonObject();
		scriptObject1.add("script", scriptObject);
		scriptObject.addProperty("file", scriptFile);
		scriptObject.addProperty("lang", "groovy");
		if (null != params) { scriptObject.add("params", params); }
		JsonObject queryDSL = new JsonObject();
		queryDSL.add(type, scriptObject1);
		return queryDSL;
	}
}
