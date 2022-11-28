package com.xuanwu.source;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @description:
 * @author: lugela
 * @create: 2022-09-04 20:02
 */

@Data
public class MyDdataJson implements Serializable {
    private JsonNode MydataValue;
    List<MydataFieldInfo> mydataFieldInfos;
}
