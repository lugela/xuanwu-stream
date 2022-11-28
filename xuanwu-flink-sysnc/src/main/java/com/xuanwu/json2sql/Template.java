package com.xuanwu.json2sql;

public @interface Template {
    String jdbcType() default "";
}
