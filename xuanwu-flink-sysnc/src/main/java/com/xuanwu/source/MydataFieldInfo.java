package com.xuanwu.source;

import lombok.Data;
import org.apache.kafka.connect.data.Schema;

import java.io.Serializable;

/**
 * @description:
 * @author: lugela
 * @create: 2022-09-04 20:16
 */
@Data
public class MydataFieldInfo implements Serializable {
    String fieldName;
    String fieldNameSchema;
    Schema.Type fieldType;

    public MydataFieldInfo(String fieldName, String fieldNameSchema, Schema.Type fieldType) {
       this.fieldName = fieldName;
       this.fieldNameSchema = fieldNameSchema;
       this.fieldType = fieldType;
    }
}
