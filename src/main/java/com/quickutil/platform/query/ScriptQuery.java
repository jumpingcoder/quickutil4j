package com.quickutil.platform.query;

import com.google.gson.JsonObject;
import com.quickutil.platform.FormatQueryException;
import com.quickutil.platform.JsonUtil;

/**
 * @author shijie.ruan
 */
public class ScriptQuery extends QueryDSL {
	private ScriptLang scriptLang = null;
	private ScriptType scriptType = null;
	private String script = null;
	private JsonObject params = null;

	ScriptQuery(ScriptType scriptType, String script) {
		super("script");
		this.scriptType = scriptType;
		this.script = script;
	}

	ScriptQuery(ScriptType scriptType, ScriptLang scriptLang, String script) {
		super("script");
		this.scriptType = scriptType;
		this.scriptLang = scriptLang;
		this.script = script;
	}

	ScriptQuery(ScriptType scriptType, ScriptLang scriptLang, JsonObject params) {
		super("script");
		this.scriptType = scriptType;
		this.scriptLang = scriptLang;
		this.params = params;
	}

	ScriptQuery(ScriptType scriptType, ScriptLang scriptLang, JsonObject params, String script) {
		super("script");
		this.scriptType = scriptType;
		this.scriptLang = scriptLang;
		this.params = params;
		this.script = script;
	}

	public ScriptQuery setScript(String script) {
		this.script = script;
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
	public String toJson() throws FormatQueryException {
		JsonObject scriptObject1 = new JsonObject(); // query api 中使用的脚本需要两层 script 包装
		JsonObject scriptObject = new JsonObject();
		scriptObject1.add("script", scriptObject);
		scriptObject.addProperty(scriptType.toString(), script);
		scriptObject.addProperty("lang", scriptLang.toString());
		scriptObject.addProperty("params", JsonUtil.toJson(params));
		JsonObject queryDSL = new JsonObject();
		queryDSL.add(type, scriptObject1);
		return JsonUtil.toJson(queryDSL);
	}

	/**
	 * 现在仅支持使用 groovy 脚本, 因为它是2.x 和5.x 都支持,而且不用装插件的脚本
	 */
	public enum ScriptLang {
		groovy
	}

	/**
	 * 脚本类型仅支持这两种, 2.x 的 id 类型5.x 不支持; 5.x 支持的 stored 类型2.x 不支持
	 */
	public enum ScriptType {
		inline, file
	}
}
