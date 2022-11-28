package com.xuanwu.source;

/**
 * @description:
 * @author: lugela
 * @create: 2022-09-04 18:06
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Collections;
import java.util.Set;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerializer implements Serializer<MyDdataJson> {
    private final ObjectMapper objectMapper;

    public JsonSerializer() {
        this(Collections.emptySet(), JsonNodeFactory.withExactBigDecimals(true));
    }

    JsonSerializer(Set<SerializationFeature> serializationFeatures, JsonNodeFactory jsonNodeFactory) {
        this.objectMapper = new ObjectMapper();
        ObjectMapper var10001 = this.objectMapper;
        serializationFeatures.forEach(var10001::enable);
        this.objectMapper.setNodeFactory(jsonNodeFactory);
    }

    public byte[] serialize(String topic, MyDdataJson data) {
        if (data == null) {
            return null;
        } else {
            try {
                return this.objectMapper.writeValueAsBytes(data);
            } catch (Exception var4) {
                throw new SerializationException("Error serializing JSON message", var4);
            }
        }
    }
}
