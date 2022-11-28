package com.xuanwu.sink.jdbc.config;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

/**
 * @description:
 * @author: lugela
 * @create: 2022-09-01 13:56
 */

@Data
public class JdbcParameters implements Serializable {
    public String password;
    public String driverClassName;
    public String jdbcUrl;
    public String username;
    public Map<String,String> sourc2targetTableName;
    public Map<String,String> targetTableKey;

}
