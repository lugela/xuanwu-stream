package com.xuanwu.source;

/**
 * @description:
 * @author: lugela
 * @create: 2022-09-04 18:07
 */

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Collections;
import java.util.Set;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer implements Deserializer<JsonNode> {
    private final ObjectMapper objectMapper;

    public JsonDeserializer() {
        this(Collections.emptySet(), JsonNodeFactory.withExactBigDecimals(true));
    }

    JsonDeserializer(Set<DeserializationFeature> deserializationFeatures, JsonNodeFactory jsonNodeFactory) {
        this.objectMapper = new ObjectMapper();
        ObjectMapper var10001 = this.objectMapper;
        deserializationFeatures.forEach(var10001::enable);
        this.objectMapper.setNodeFactory(jsonNodeFactory);
    }

    public JsonNode deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        } else {
            try {
                JsonNode data = this.objectMapper.readTree(bytes);
                return data;
            } catch (Exception var5) {
                throw new SerializationException(var5);
            }
        }
    }
}

