package com.quickutil.platform.def;

import com.quickutil.platform.JsonUtil;

public class Result {

    private boolean success = false;
    private Object message;

    public Result(boolean success, Object message) {
        this.success = success;
        this.message = message;
    }

    public boolean getSuccess() {
        return success;
    }

    public Object getMessage() {
        return message;
    }

    public String toJson() {
        return JsonUtil.toJson(this);
    }

    public Result clone() {
        return new Result(success, message);
    }
}
