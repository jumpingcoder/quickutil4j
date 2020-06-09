package com.quickutil.platform;

import static net.logstash.logback.argument.StructuredArguments.value;

import org.slf4j.Logger;

public class LogUtil {

	private static Model _model = Model.console;

	public static void init(Model model) {
		_model = model;
	}

	;

	public static void trace(Logger logger, String message, Object toJson) {
		if (_model == Model.console) {
			logger.trace(message);
			logger.trace(JsonUtil.toJson(toJson));
		} else {
			logger.trace(message + "::{}", value("json", toJson));
		}
	}

	public static void debug(Logger logger, String message, Object toJson) {
		if (_model == Model.console) {
			logger.debug(message);
			logger.debug(JsonUtil.toJson(toJson));
		} else {
			logger.debug(message + "::{}", value("json", toJson));
		}
	}

	public static void info(Logger logger, String message, Object toJson) {
		if (_model == Model.console) {
			logger.info(message);
			logger.info(JsonUtil.toJson(toJson));
		} else {
			logger.info(message + "::{}", value("json", toJson));
		}
	}

	public static void warn(Logger logger, String message, Object toJson) {
		if (_model == Model.console) {
			logger.warn(message);
			logger.warn(JsonUtil.toJson(toJson));
		} else {
			logger.warn(message + "::{}", value("json", toJson));
		}
	}

	public static void error(Logger logger, String message, Object toJson) {
		if (_model == Model.console) {
			logger.error(message);
			logger.error(JsonUtil.toJson(toJson));
		} else {
			logger.error(message + "::{}", value("json", toJson));
		}
	}

	public enum Model {console, json}
}
