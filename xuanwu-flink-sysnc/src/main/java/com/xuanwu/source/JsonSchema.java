package com.xuanwu.source;

/**
 * @description:
 * @author: lugela
 * @create: 2022-09-04 17:45
 */

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonSchema {
    static final String ENVELOPE_SCHEMA_FIELD_NAME = "schema";
    static final String ENVELOPE_PAYLOAD_FIELD_NAME = "payload";
    static final String SCHEMA_TYPE_FIELD_NAME = "type";
    static final String SCHEMA_OPTIONAL_FIELD_NAME = "optional";
    static final String SCHEMA_NAME_FIELD_NAME = "name";
    static final String SCHEMA_VERSION_FIELD_NAME = "version";
    static final String SCHEMA_DOC_FIELD_NAME = "doc";
    static final String SCHEMA_PARAMETERS_FIELD_NAME = "parameters";
    static final String SCHEMA_DEFAULT_FIELD_NAME = "default";
    static final String ARRAY_ITEMS_FIELD_NAME = "items";
    static final String MAP_KEY_FIELD_NAME = "keys";
    static final String MAP_VALUE_FIELD_NAME = "values";
    static final String STRUCT_FIELDS_FIELD_NAME = "fields";
    static final String STRUCT_FIELD_NAME_FIELD_NAME = "field";
    static final String BOOLEAN_TYPE_NAME = "boolean";
    static final ObjectNode BOOLEAN_SCHEMA;
    static final String INT8_TYPE_NAME = "int8";
    static final ObjectNode INT8_SCHEMA;
    static final String INT16_TYPE_NAME = "int16";
    static final ObjectNode INT16_SCHEMA;
    static final String INT32_TYPE_NAME = "int32";
    static final ObjectNode INT32_SCHEMA;
    static final String INT64_TYPE_NAME = "int64";
    static final ObjectNode INT64_SCHEMA;
    static final String FLOAT_TYPE_NAME = "float";
    static final ObjectNode FLOAT_SCHEMA;
    static final String DOUBLE_TYPE_NAME = "double";
    static final ObjectNode DOUBLE_SCHEMA;
    static final String BYTES_TYPE_NAME = "bytes";
    static final ObjectNode BYTES_SCHEMA;
    static final String STRING_TYPE_NAME = "string";
    static final ObjectNode STRING_SCHEMA;
    static final String ARRAY_TYPE_NAME = "array";
    static final String MAP_TYPE_NAME = "map";
    static final String STRUCT_TYPE_NAME = "struct";

    public JsonSchema() {
    }

    public static ObjectNode envelope(JsonNode schema, JsonNode payload) {
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        result.set("schema", schema);
        result.set("payload", payload);
        return result;
    }

    static {
        BOOLEAN_SCHEMA = JsonNodeFactory.instance.objectNode().put("type", "boolean");
        INT8_SCHEMA = JsonNodeFactory.instance.objectNode().put("type", "int8");
        INT16_SCHEMA = JsonNodeFactory.instance.objectNode().put("type", "int16");
        INT32_SCHEMA = JsonNodeFactory.instance.objectNode().put("type", "int32");
        INT64_SCHEMA = JsonNodeFactory.instance.objectNode().put("type", "int64");
        FLOAT_SCHEMA = JsonNodeFactory.instance.objectNode().put("type", "float");
        DOUBLE_SCHEMA = JsonNodeFactory.instance.objectNode().put("type", "double");
        BYTES_SCHEMA = JsonNodeFactory.instance.objectNode().put("type", "bytes");
        STRING_SCHEMA = JsonNodeFactory.instance.objectNode().put("type", "string");
    }

    static class Envelope {
        public JsonNode schema;
        public JsonNode payload;

        public Envelope(JsonNode schema, JsonNode payload) {
            this.schema = schema;
            this.payload = payload;
        }

        public ObjectNode toJsonNode() {
            return org.apache.kafka.connect.json.JsonSchema.envelope(this.schema, this.payload);
        }
    }
}

