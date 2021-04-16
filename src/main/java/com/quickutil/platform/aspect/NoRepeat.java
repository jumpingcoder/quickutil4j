package com.quickutil.platform.aspect;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface NoRepeat {

	int lockExpire() default 86400;//默认一天过期

	String taskName() default "";
}
