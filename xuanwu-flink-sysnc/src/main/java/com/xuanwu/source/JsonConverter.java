package com.xuanwu.source;

/**
 * @description:
 * @author: lugela
 * @create: 2022-09-04 17:38
 */

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.HeaderConverter;

public class JsonConverter implements Converter, HeaderConverter {
    private static final Map<Type, JsonToConnectTypeConverter> TO_CONNECT_CONVERTERS = new EnumMap(Type.class);
    private static final HashMap<String, LogicalTypeConverter> LOGICAL_CONVERTERS;
    private static final JsonNodeFactory JSON_NODE_FACTORY;
    private JsonConverterConfig config;
    private Cache<Schema, ObjectNode> fromConnectSchemaCache;
    private Cache<JsonNode, Schema> toConnectSchemaCache;
    private final JsonSerializer serializer;
    private final JsonDeserializer deserializer;

    public JsonConverter() {
        this.serializer = new JsonSerializer(Utils.mkSet(new SerializationFeature[0]), JSON_NODE_FACTORY);
        this.deserializer = new JsonDeserializer(Utils.mkSet(new DeserializationFeature[]{DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS}), JSON_NODE_FACTORY);
    }

    public ConfigDef config() {
        return JsonConverterConfig.configDef();
    }

    public void configure(Map<String, ?> configs) {
        this.config = new JsonConverterConfig(configs);
        this.serializer.configure(configs, this.config.type() == ConverterType.KEY);
        this.deserializer.configure(configs, this.config.type() == ConverterType.KEY);
        this.fromConnectSchemaCache = new SynchronizedCache(new LRUCache(this.config.schemaCacheSize()));
        this.toConnectSchemaCache = new SynchronizedCache(new LRUCache(this.config.schemaCacheSize()));
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> conf = new HashMap(configs);
        conf.put("converter.type", isKey ? ConverterType.KEY.getName() : ConverterType.VALUE.getName());
        this.configure(conf);
    }

    public void close() {
    }

    public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
        return this.fromConnectData(topic, schema, value);
    }

    public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
        return this.toConnectData(topic, value);
    }

    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema == null && value == null) {
            return null;
        } else {
            MyDdataJson myDdataJson = new MyDdataJson();
            JsonNode jsonValue = this.config.schemasEnabled() ? this.convertToJsonWithEnvelope(schema, value) : this.convertToJsonWithoutEnvelope(schema, value);
            //加入字段的映射关系，方便后面转换
            Field afterField = schema.field("after");
            Field beforeField = schema.field("before");
            Field fieldNew = (null !=afterField) ? afterField : beforeField;
             List<Field> fields = fieldNew.schema().fields();
            List<MydataFieldInfo> fieldInfos = new ArrayList<>();
            for (Field field:fields){
                //fieldName
                String fieldName = field.name();
                Schema fieldSchema = field.schema();
                String fieldNameSchema = fieldSchema.name();
                Type fieldType = fieldSchema.type();
                MydataFieldInfo mydataFieldInfo = new MydataFieldInfo(fieldName,fieldNameSchema,fieldType);
                fieldInfos.add(mydataFieldInfo);
            }

            myDdataJson.setMydataValue(jsonValue);
            myDdataJson.setMydataFieldInfos(fieldInfos);



            try {
                return this.serializer.serialize(topic, myDdataJson);
            } catch (SerializationException var6) {
                throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", var6);
            }
        }
    }

    public SchemaAndValue toConnectData(String topic, byte[] value) {
        if (value == null) {
            return SchemaAndValue.NULL;
        } else {
            Object jsonValue;
            try {
                jsonValue = this.deserializer.deserialize(topic, value);
            } catch (SerializationException var5) {
                throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", var5);
            }

            if (this.config.schemasEnabled() && (!((JsonNode)jsonValue).isObject() || ((JsonNode)jsonValue).size() != 2 || !((JsonNode)jsonValue).has("schema") || !((JsonNode)jsonValue).has("payload"))) {
                throw new DataException("JsonConverter with schemas.enable requires \"schema\" and \"payload\" fields and may not contain additional fields. If you are trying to deserialize plain JSON data, set schemas.enable=false in your converter configuration.");
            } else {
                if (!this.config.schemasEnabled()) {
                    ObjectNode envelope = JSON_NODE_FACTORY.objectNode();
                    envelope.set("schema", (JsonNode)null);
                    envelope.set("payload", (JsonNode)jsonValue);
                    jsonValue = envelope;
                }

                Schema schema = this.asConnectSchema(((JsonNode)jsonValue).get("schema"));
                return new SchemaAndValue(schema, convertToConnect(schema, ((JsonNode)jsonValue).get("payload")));
            }
        }
    }

    public ObjectNode asJsonSchema(Schema schema) {
        if (schema == null) {
            return null;
        } else {
            ObjectNode cached = (ObjectNode)this.fromConnectSchemaCache.get(schema);
            if (cached != null) {
                return cached;
            } else {
                ObjectNode jsonSchema;
                Iterator var5;
                switch(schema.type()) {
                    case BOOLEAN:
                        jsonSchema = JsonSchema.BOOLEAN_SCHEMA.deepCopy();
                        break;
                    case BYTES:
                        jsonSchema = JsonSchema.BYTES_SCHEMA.deepCopy();
                        break;
                    case FLOAT64:
                        jsonSchema = JsonSchema.DOUBLE_SCHEMA.deepCopy();
                        break;
                    case FLOAT32:
                        jsonSchema = JsonSchema.FLOAT_SCHEMA.deepCopy();
                        break;
                    case INT8:
                        jsonSchema = JsonSchema.INT8_SCHEMA.deepCopy();
                        break;
                    case INT16:
                        jsonSchema = JsonSchema.INT16_SCHEMA.deepCopy();
                        break;
                    case INT32:
                        jsonSchema = JsonSchema.INT32_SCHEMA.deepCopy();
                        break;
                    case INT64:
                        jsonSchema = JsonSchema.INT64_SCHEMA.deepCopy();
                        break;
                    case STRING:
                        jsonSchema = JsonSchema.STRING_SCHEMA.deepCopy();
                        break;
                    case ARRAY:
                        jsonSchema = JSON_NODE_FACTORY.objectNode().put("type", "array");
                        jsonSchema.set("items", this.asJsonSchema(schema.valueSchema()));
                        break;
                    case MAP:
                        jsonSchema = JSON_NODE_FACTORY.objectNode().put("type", "map");
                        jsonSchema.set("keys", this.asJsonSchema(schema.keySchema()));
                        jsonSchema.set("values", this.asJsonSchema(schema.valueSchema()));
                        break;
                    case STRUCT:
                        jsonSchema = JSON_NODE_FACTORY.objectNode().put("type", "struct");
                        ArrayNode fields = JSON_NODE_FACTORY.arrayNode();
                        var5 = schema.fields().iterator();

                        while(var5.hasNext()) {
                            Field field = (Field)var5.next();
                            ObjectNode fieldJsonSchema = this.asJsonSchema(field.schema()).deepCopy();
                            fieldJsonSchema.put("field", field.name());
                            fields.add(fieldJsonSchema);
                        }

                        jsonSchema.set("fields", fields);
                        break;
                    default:
                        throw new DataException("Couldn't translate unsupported schema type " + schema + ".");
                }

                jsonSchema.put("optional", schema.isOptional());
                if (schema.name() != null) {
                    jsonSchema.put("name", schema.name());
                }

                if (schema.version() != null) {
                    jsonSchema.put("version", schema.version());
                }

                if (schema.doc() != null) {
                    jsonSchema.put("doc", schema.doc());
                }

                if (schema.parameters() != null) {
                    ObjectNode jsonSchemaParams = JSON_NODE_FACTORY.objectNode();
                    var5 = schema.parameters().entrySet().iterator();

                    while(var5.hasNext()) {
                        Entry<String, String> prop = (Entry)var5.next();
                        jsonSchemaParams.put((String)prop.getKey(), (String)prop.getValue());
                    }

                    jsonSchema.set("parameters", jsonSchemaParams);
                }

                if (schema.defaultValue() != null) {
                    jsonSchema.set("default", this.convertToJson(schema, schema.defaultValue()));
                }

                this.fromConnectSchemaCache.put(schema, jsonSchema);
                return jsonSchema;
            }
        }
    }

    public Schema asConnectSchema(JsonNode jsonSchema) {
        if (jsonSchema.isNull()) {
            return null;
        } else {
            Schema cached = (Schema)this.toConnectSchemaCache.get(jsonSchema);
            if (cached != null) {
                return cached;
            } else {
                JsonNode schemaTypeNode = jsonSchema.get("type");
                if (schemaTypeNode != null && schemaTypeNode.isTextual()) {
                    String var5 = schemaTypeNode.textValue();
                    byte var6 = -1;
                    switch(var5.hashCode()) {
                        case -1325958191:
                            if (var5.equals("double")) {
                                var6 = 6;
                            }
                            break;
                        case -891985903:
                            if (var5.equals("string")) {
                                var6 = 8;
                            }
                            break;
                        case -891974699:
                            if (var5.equals("struct")) {
                                var6 = 11;
                            }
                            break;
                        case 107868:
                            if (var5.equals("map")) {
                                var6 = 10;
                            }
                            break;
                        case 3237417:
                            if (var5.equals("int8")) {
                                var6 = 1;
                            }
                            break;
                        case 64711720:
                            if (var5.equals("boolean")) {
                                var6 = 0;
                            }
                            break;
                        case 93090393:
                            if (var5.equals("array")) {
                                var6 = 9;
                            }
                            break;
                        case 94224491:
                            if (var5.equals("bytes")) {
                                var6 = 7;
                            }
                            break;
                        case 97526364:
                            if (var5.equals("float")) {
                                var6 = 5;
                            }
                            break;
                        case 100359764:
                            if (var5.equals("int16")) {
                                var6 = 2;
                            }
                            break;
                        case 100359822:
                            if (var5.equals("int32")) {
                                var6 = 3;
                            }
                            break;
                        case 100359917:
                            if (var5.equals("int64")) {
                                var6 = 4;
                            }
                    }

                    SchemaBuilder builder;
                    JsonNode schemaVersionNode;
                    JsonNode schemaDocNode;
                    JsonNode schemaParamsNode;
                    JsonNode schemaDefaultNode;
                    JsonNode paramValue;
                    label148:
                    switch(var6) {
                        case 0:
                            builder = SchemaBuilder.bool();
                            break;
                        case 1:
                            builder = SchemaBuilder.int8();
                            break;
                        case 2:
                            builder = SchemaBuilder.int16();
                            break;
                        case 3:
                            builder = SchemaBuilder.int32();
                            break;
                        case 4:
                            builder = SchemaBuilder.int64();
                            break;
                        case 5:
                            builder = SchemaBuilder.float32();
                            break;
                        case 6:
                            builder = SchemaBuilder.float64();
                            break;
                        case 7:
                            builder = SchemaBuilder.bytes();
                            break;
                        case 8:
                            builder = SchemaBuilder.string();
                            break;
                        case 9:
                            schemaVersionNode = jsonSchema.get("items");
                            if (schemaVersionNode != null && !schemaVersionNode.isNull()) {
                                builder = SchemaBuilder.array(this.asConnectSchema(schemaVersionNode));
                                break;
                            }

                            throw new DataException("Array schema did not specify the element type");
                        case 10:
                            schemaDocNode = jsonSchema.get("keys");
                            if (schemaDocNode == null) {
                                throw new DataException("Map schema did not specify the key type");
                            }

                            schemaParamsNode = jsonSchema.get("values");
                            if (schemaParamsNode == null) {
                                throw new DataException("Map schema did not specify the value type");
                            }

                            builder = SchemaBuilder.map(this.asConnectSchema(schemaDocNode), this.asConnectSchema(schemaParamsNode));
                            break;
                        case 11:
                            builder = SchemaBuilder.struct();
                            schemaDefaultNode = jsonSchema.get("fields");
                            if (schemaDefaultNode == null || !schemaDefaultNode.isArray()) {
                                throw new DataException("Struct schema's \"fields\" argument is not an array.");
                            }

                            Iterator var11 = schemaDefaultNode.iterator();

                            while(true) {
                                if (!var11.hasNext()) {
                                    break label148;
                                }

                                paramValue = (JsonNode)var11.next();
                                JsonNode jsonFieldName = paramValue.get("field");
                                if (jsonFieldName == null || !jsonFieldName.isTextual()) {
                                    throw new DataException("Struct schema's field name not specified properly");
                                }

                                builder.field(jsonFieldName.asText(), this.asConnectSchema(paramValue));
                            }
                        default:
                            throw new DataException("Unknown schema type: " + schemaTypeNode.textValue());
                    }

                    JsonNode schemaOptionalNode = jsonSchema.get("optional");
                    if (schemaOptionalNode != null && schemaOptionalNode.isBoolean() && schemaOptionalNode.booleanValue()) {
                        builder.optional();
                    } else {
                        builder.required();
                    }

                    JsonNode schemaNameNode = jsonSchema.get("name");
                    if (schemaNameNode != null && schemaNameNode.isTextual()) {
                        builder.name(schemaNameNode.textValue());
                    }

                    schemaVersionNode = jsonSchema.get("version");
                    if (schemaVersionNode != null && schemaVersionNode.isIntegralNumber()) {
                        builder.version(schemaVersionNode.intValue());
                    }

                    schemaDocNode = jsonSchema.get("doc");
                    if (schemaDocNode != null && schemaDocNode.isTextual()) {
                        builder.doc(schemaDocNode.textValue());
                    }

                    schemaParamsNode = jsonSchema.get("parameters");
                    if (schemaParamsNode != null && schemaParamsNode.isObject()) {
                        Iterator paramsIt = schemaParamsNode.fields();

                        while(paramsIt.hasNext()) {
                            Entry<String, JsonNode> entry = (Entry)paramsIt.next();
                            paramValue = (JsonNode)entry.getValue();
                            if (!paramValue.isTextual()) {
                                throw new DataException("Schema parameters must have string values.");
                            }

                            builder.parameter((String)entry.getKey(), paramValue.textValue());
                        }
                    }

                    schemaDefaultNode = jsonSchema.get("default");
                    if (schemaDefaultNode != null) {
                        builder.defaultValue(convertToConnect(builder, schemaDefaultNode));
                    }

                    Schema result = builder.build();
                    this.toConnectSchemaCache.put(jsonSchema, result);
                    return result;
                } else {
                    throw new DataException("Schema must contain 'type' field");
                }
            }
        }
    }

    private JsonNode convertToJsonWithEnvelope(Schema schema, Object value) {
        return (new JsonSchema.Envelope(this.asJsonSchema(schema), this.convertToJson(schema, value))).toJsonNode();
    }

    private JsonNode convertToJsonWithoutEnvelope(Schema schema, Object value) {
        return this.convertToJson(schema, value);
    }

    private JsonNode convertToJson(Schema schema, Object value) {
        if (value == null) {
            if (schema == null) {
                return null;
            } else if (schema.defaultValue() != null) {
                return this.convertToJson(schema, schema.defaultValue());
            } else if (schema.isOptional()) {
                return JSON_NODE_FACTORY.nullNode();
            } else {
                throw new DataException("Conversion error: null value for field that is required and has no default value");
            }
        } else {
            if (schema != null && schema.name() != null) {
                LogicalTypeConverter logicalConverter = (LogicalTypeConverter)LOGICAL_CONVERTERS.get(schema.name());
                if (logicalConverter != null) {
                    return logicalConverter.toJson(schema, value, this.config);
                }
            }

            try {
                Type schemaType;
                if (schema == null) {
                    schemaType = ConnectSchema.schemaType(value.getClass());
                    if (schemaType == null) {
                        throw new DataException("Java class " + value.getClass() + " does not have corresponding schema type.");
                    }
                } else {
                    schemaType = schema.type();
                }

                Iterator var7;
                switch(schemaType) {
                    case BOOLEAN:
                        return JSON_NODE_FACTORY.booleanNode((Boolean)value);
                    case BYTES:
                        if (value instanceof byte[]) {
                            return JSON_NODE_FACTORY.binaryNode((byte[])((byte[])value));
                        } else {
                            if (value instanceof ByteBuffer) {
                                return JSON_NODE_FACTORY.binaryNode(((ByteBuffer)value).array());
                            }

                            throw new DataException("Invalid type for bytes type: " + value.getClass());
                        }
                    case FLOAT64:
                        return JSON_NODE_FACTORY.numberNode((Double)value);
                    case FLOAT32:
                        return JSON_NODE_FACTORY.numberNode((Float)value);
                    case INT8:
                        return JSON_NODE_FACTORY.numberNode((Byte)value);
                    case INT16:
                        return JSON_NODE_FACTORY.numberNode((Short)value);
                    case INT32:
                        return JSON_NODE_FACTORY.numberNode((Integer)value);
                    case INT64:
                        return JSON_NODE_FACTORY.numberNode((Long)value);
                    case STRING:
                        CharSequence charSeq = (CharSequence)value;
                        return JSON_NODE_FACTORY.textNode(charSeq.toString());
                    case ARRAY:
                        Collection collection = (Collection)value;
                        ArrayNode list1 = JSON_NODE_FACTORY.arrayNode();
                        var7 = collection.iterator();

                        while(var7.hasNext()) {
                            Object elem = var7.next();
                            Schema valueSchema = schema == null ? null : schema.valueSchema();
                            JsonNode fieldValue = this.convertToJson(valueSchema, elem);
                            list1.add(fieldValue);
                        }

                        return list1;
                    case MAP:
                        Map<?, ?> map = (Map)value;
                        boolean objectMode;
                        if (schema == null) {
                            objectMode = true;
                            var7 = map.entrySet().iterator();

                            while(var7.hasNext()) {
                                Entry<?, ?> entry = (Entry)var7.next();
                                if (!(entry.getKey() instanceof String)) {
                                    objectMode = false;
                                    break;
                                }
                            }
                        } else {
                            objectMode = schema.keySchema().type() == Type.STRING;
                        }

                        ObjectNode obj = null;
                        ArrayNode list = null;
                        if (objectMode) {
                            obj = JSON_NODE_FACTORY.objectNode();
                        } else {
                             list = JSON_NODE_FACTORY.arrayNode();
                        }

                        Iterator var9 = map.entrySet().iterator();

                        while(var9.hasNext()) {
                            Entry<?, ?> entry = (Entry)var9.next();
                            Schema keySchema = schema == null ? null : schema.keySchema();
                            Schema valueSchema = schema == null ? null : schema.valueSchema();
                            JsonNode mapKey = this.convertToJson(keySchema, entry.getKey());
                            JsonNode mapValue = this.convertToJson(valueSchema, entry.getValue());
                            if (objectMode) {
                                obj.set(mapKey.asText(), mapValue);
                            } else {
                                list.add(JSON_NODE_FACTORY.arrayNode().add(mapKey).add(mapValue));
                            }
                        }

                        return (JsonNode)(objectMode ? obj : list);
                    case STRUCT:
                        Struct struct = (Struct)value;
                        if (!struct.schema().equals(schema)) {
                            throw new DataException("Mismatching schema.");
                        }

                        ObjectNode obj1 = JSON_NODE_FACTORY.objectNode();
                        var7 = schema.fields().iterator();

                        while(var7.hasNext()) {
                            Field field = (Field)var7.next();
                            obj1.set(field.name(), this.convertToJson(field.schema(), struct.get(field)));
                        }

                        return obj1;
                    default:
                        throw new DataException("Couldn't convert " + value + " to JSON.");
                }
            } catch (ClassCastException var15) {
                String schemaTypeStr = schema != null ? schema.type().toString() : "unknown schema";
                throw new DataException("Invalid type for " + schemaTypeStr + ": " + value.getClass());
            }
        }
    }

    private static Object convertToConnect(Schema schema, JsonNode jsonValue) {
        Type schemaType;
        if (schema != null) {
            schemaType = schema.type();
            if (jsonValue == null || jsonValue.isNull()) {
                if (schema.defaultValue() != null) {
                    return schema.defaultValue();
                } else if (schema.isOptional()) {
                    return null;
                } else {
                    throw new DataException("Invalid null value for required " + schemaType + " field");
                }
            }
        } else {
            switch(jsonValue.getNodeType()) {
                case NULL:
                case MISSING:
                    return null;
                case BOOLEAN:
                    schemaType = Type.BOOLEAN;
                    break;
                case NUMBER:
                    if (jsonValue.isIntegralNumber()) {
                        schemaType = Type.INT64;
                    } else {
                        schemaType = Type.FLOAT64;
                    }
                    break;
                case ARRAY:
                    schemaType = Type.ARRAY;
                    break;
                case OBJECT:
                    schemaType = Type.MAP;
                    break;
                case STRING:
                    schemaType = Type.STRING;
                    break;
                case BINARY:
                case POJO:
                default:
                    schemaType = null;
            }
        }

        JsonToConnectTypeConverter typeConverter = (JsonToConnectTypeConverter)TO_CONNECT_CONVERTERS.get(schemaType);
        if (typeConverter == null) {
            throw new DataException("Unknown schema type: " + schemaType);
        } else {
            if (schema != null && schema.name() != null) {
                LogicalTypeConverter logicalConverter = (LogicalTypeConverter)LOGICAL_CONVERTERS.get(schema.name());
                if (logicalConverter != null) {
                    return logicalConverter.toConnect(schema, jsonValue);
                }
            }

            return typeConverter.convert(schema, jsonValue);
        }
    }

    static {
        TO_CONNECT_CONVERTERS.put(Type.BOOLEAN, new JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                return value.booleanValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Type.INT8, new JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                return (byte)value.intValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Type.INT16, new JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                return (short)value.intValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Type.INT32, new JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                return value.intValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Type.INT64, new JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                return value.longValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Type.FLOAT32, new JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                return value.floatValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Type.FLOAT64, new JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                return value.doubleValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Type.BYTES, new JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                try {
                    return value.binaryValue();
                } catch (IOException var4) {
                    throw new DataException("Invalid bytes field", var4);
                }
            }
        });
        TO_CONNECT_CONVERTERS.put(Type.STRING, new JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                return value.textValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Type.ARRAY, new JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                Schema elemSchema = schema == null ? null : schema.valueSchema();
                ArrayList<Object> result = new ArrayList();
                Iterator var5 = value.iterator();

                while(var5.hasNext()) {
                    JsonNode elem = (JsonNode)var5.next();
                    result.add(convertToConnect(elemSchema, elem));
                }

                return result;
            }
        });
        TO_CONNECT_CONVERTERS.put(Type.MAP, new JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                Schema keySchema = schema == null ? null : schema.keySchema();
                Schema valueSchema = schema == null ? null : schema.valueSchema();
                Map<Object, Object> result = new HashMap();
                Iterator fieldIt;
                if (schema != null && keySchema.type() != Type.STRING) {
                    if (!value.isArray()) {
                        throw new DataException("Maps with non-string fields should be encoded as JSON array of tuples, but found " + value.getNodeType());
                    }

                    fieldIt = value.iterator();

                    while(fieldIt.hasNext()) {
                        JsonNode entryx = (JsonNode)fieldIt.next();
                        if (!entryx.isArray()) {
                            throw new DataException("Found invalid map entry instead of array tuple: " + entryx.getNodeType());
                        }

                        if (entryx.size() != 2) {
                            throw new DataException("Found invalid map entry, expected length 2 but found :" + entryx.size());
                        }

                        result.put(convertToConnect(keySchema, entryx.get(0)), convertToConnect(valueSchema, entryx.get(1)));
                    }
                } else {
                    if (!value.isObject()) {
                        throw new DataException("Maps with string fields should be encoded as JSON objects, but found " + value.getNodeType());
                    }

                    fieldIt = value.fields();

                    while(fieldIt.hasNext()) {
                        Entry<String, JsonNode> entry = (Entry)fieldIt.next();
                        result.put(entry.getKey(), convertToConnect(valueSchema, (JsonNode)entry.getValue()));
                    }
                }

                return result;
            }
        });
        TO_CONNECT_CONVERTERS.put(Type.STRUCT, new JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                if (!value.isObject()) {
                    throw new DataException("Structs should be encoded as JSON objects, but found " + value.getNodeType());
                } else {
                    Struct result = new Struct(schema.schema());
                    Iterator var4 = schema.fields().iterator();

                    while(var4.hasNext()) {
                        Field field = (Field)var4.next();
                        result.put(field, convertToConnect(field.schema(), value.get(field.name())));
                    }

                    return result;
                }
            }
        });
        LOGICAL_CONVERTERS = new HashMap();
        JSON_NODE_FACTORY = JsonNodeFactory.withExactBigDecimals(true);
        LOGICAL_CONVERTERS.put("org.apache.kafka.connect.data.Decimal", new LogicalTypeConverter() {
            public JsonNode toJson(Schema schema, Object value, JsonConverterConfig config) {
                if (!(value instanceof BigDecimal)) {
                    throw new DataException("Invalid type for Decimal, expected BigDecimal but was " + value.getClass());
                } else {
                    BigDecimal decimal = (BigDecimal)value;
                    switch(config.decimalFormat()) {
                        case NUMERIC:
                            return JSON_NODE_FACTORY.numberNode(decimal);
                        case BASE64:
                            return JSON_NODE_FACTORY.binaryNode(Decimal.fromLogical(schema, decimal));
                        default:
                            throw new DataException("Unexpected decimal.format: " + config.decimalFormat());
                    }
                }
            }

            public Object toConnect(Schema schema, JsonNode value) {
                if (value.isNumber()) {
                    return value.decimalValue();
                } else if (!value.isBinary() && !value.isTextual()) {
                    throw new DataException("Invalid type for Decimal, underlying representation should be numeric or bytes but was " + value.getNodeType());
                } else {
                    try {
                        return Decimal.toLogical(schema, value.binaryValue());
                    } catch (Exception var4) {
                        throw new DataException("Invalid bytes for Decimal field", var4);
                    }
                }
            }
        });
        LOGICAL_CONVERTERS.put("org.apache.kafka.connect.data.Date", new LogicalTypeConverter() {
            public JsonNode toJson(Schema schema, Object value, JsonConverterConfig config) {
                if (!(value instanceof Date)) {
                    throw new DataException("Invalid type for Date, expected Date but was " + value.getClass());
                } else {
                    return JSON_NODE_FACTORY.numberNode(org.apache.kafka.connect.data.Date.fromLogical(schema, (Date)value));
                }
            }

            public Object toConnect(Schema schema, JsonNode value) {
                if (!value.isInt()) {
                    throw new DataException("Invalid type for Date, underlying representation should be integer but was " + value.getNodeType());
                } else {
                    return org.apache.kafka.connect.data.Date.toLogical(schema, value.intValue());
                }
            }
        });
        LOGICAL_CONVERTERS.put("org.apache.kafka.connect.data.Time", new LogicalTypeConverter() {
            public JsonNode toJson(Schema schema, Object value, JsonConverterConfig config) {
                if (!(value instanceof Date)) {
                    throw new DataException("Invalid type for Time, expected Date but was " + value.getClass());
                } else {
                    return JSON_NODE_FACTORY.numberNode(Time.fromLogical(schema, (Date)value));
                }
            }

            public Object toConnect(Schema schema, JsonNode value) {
                if (!value.isInt()) {
                    throw new DataException("Invalid type for Time, underlying representation should be integer but was " + value.getNodeType());
                } else {
                    return Time.toLogical(schema, value.intValue());
                }
            }
        });
        LOGICAL_CONVERTERS.put("org.apache.kafka.connect.data.Timestamp", new LogicalTypeConverter() {
            public JsonNode toJson(Schema schema, Object value, JsonConverterConfig config) {
                if (!(value instanceof Date)) {
                    throw new DataException("Invalid type for Timestamp, expected Date but was " + value.getClass());
                } else {
                    return JSON_NODE_FACTORY.numberNode(Timestamp.fromLogical(schema, (Date)value));
                }
            }

            public Object toConnect(Schema schema, JsonNode value) {
                if (!value.isIntegralNumber()) {
                    throw new DataException("Invalid type for Timestamp, underlying representation should be integral but was " + value.getNodeType());
                } else {
                    return Timestamp.toLogical(schema, value.longValue());
                }
            }
        });
    }

    private interface LogicalTypeConverter {
        JsonNode toJson(Schema var1, Object var2, JsonConverterConfig var3);

        Object toConnect(Schema var1, JsonNode var2);
    }

    private interface JsonToConnectTypeConverter {
        Object convert(Schema var1, JsonNode var2);
    }
}
