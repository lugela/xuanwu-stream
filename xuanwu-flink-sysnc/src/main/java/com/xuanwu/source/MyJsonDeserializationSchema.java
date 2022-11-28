package com.xuanwu.source;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.data.Envelope;
import io.debezium.time.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 **
 * @description:
 * @author: lugela
 * @date: Created in 2021/8/26 18:12
 * @version: 1.0
 * @modified By:
 */

class MyJsonDeserializationSchema implements DebeziumDeserializationSchema<JSONObject> {

   private static SimpleDateFormat format = null ;
    private static SimpleDateFormat formathms = null ;
    static {
        format = new SimpleDateFormat("yyyy-MM-dd");
         formathms = new SimpleDateFormat("HH:mm:ss");

    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<JSONObject> collector) throws Exception {

        //构建结果对象
        JSONObject result = new JSONObject();

        //获取数据库名称&表名称
        String topic = sourceRecord.topic();

        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];

        //获取数据
        Struct value = (Struct) sourceRecord.value();

        //After
        Struct after = value.getStruct("after");
        JSONObject data = new JSONObject();
        if (after != null) { //delete数据,则after为null
            Schema schema = after.schema();
            List<Field> fieldList = schema.fields();


            for (int i = 0; i < fieldList.size(); i++) {
                Field field = fieldList.get(i);
                Object fieldValue = after.get(field);
                if (fieldValue != null) {
                    Object notNullConverter = createNotNullConverter(fieldValue, field.schema());
                    data.put(field.name(), notNullConverter);
                } else {
                    data.put(field.name(), fieldValue);

                }
            }
        }

        //Before
        Struct before = value.getStruct("before");
        JSONObject beforeData = new JSONObject();
        //Map<String,Object> beforeMap  = new HashMap<>();

        if (before != null) { //delete数据,则after为null
            Schema schema = before.schema();
            List<Field> fieldList = schema.fields();

            for (int i = 0; i < fieldList.size(); i++) {
                Field field = fieldList.get(i);
                Object fieldValue = before.get(field);
                if (fieldValue != null) {
                    Object notNullConverter = createNotNullConverter(fieldValue, field.schema());
                    beforeData.put(field.name(), notNullConverter);
                 //   beforeMap.put(field.name(), notNullConverter);
                } else {
                    beforeData.put(field.name(), fieldValue);
                  //  beforeMap.put(field.name(), fieldValue);
                }
            }
        }

        //获取操作类型 CREATE UPDATE DELETE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type) || "read".equals(type)) {
            type = "insert";
        }

        //封装数据
        result.put("database", database);
        result.put("tableName", tableName);
        result.put("after", data);
        result.put("before", beforeData);
        result.put("type", type);
        result.put("ts", System.currentTimeMillis());

        //输出封装好的数据
        collector.collect(result);
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
       // return BasicTypeInfo.STRING_TYPE_INFO;
        return null;
    }


    //通过映射对数据进行一个转换
    private Object createNotNullConverter(Object dbzObj, Schema schema) {
        //首先先判断出来
        Schema.Type type = schema.type();
        String name = schema.name();
        Object o=null;
        //匹配出数据格式
        switch (type) {
            /*case STRING:
                o=  convertToString(dbzObj);
                break;
            case BOOLEAN:
                o= convertToBoolean(dbzObj);
                break;
            case FLOAT32:
                o=  convertToFloat(dbzObj);
                break;
            case FLOAT64:
                o=  convertToDouble(dbzObj);
                break;
            case BYTES:
                o=  convertToBinary(dbzObj);
                break;
            case INT8:
            case INT16:*/
            case INT32:
                if (io.debezium.time.Date.SCHEMA_NAME.equals(name)) {
                    //需要转成时间格式
                    o=  convertToDate(dbzObj);
                } else {
                    o=  convertToInt(dbzObj);
                }
                break;
            case INT64:
                /*if (MicroTime.SCHEMA_NAME.equals(name) || NanoTime.SCHEMA_NAME.equals(name)) {
                    o= convertToTime(dbzObj, schema);
                } else*/ if (Timestamp.SCHEMA_NAME.equals(name) || MicroTimestamp.SCHEMA_NAME.equals(name) || NanoTimestamp.SCHEMA_NAME.equals(name)) {
                    o= convertToTimestamp(dbzObj);
                } else {
                    o=  convertToLong(dbzObj);
                }
                break;
            default:
                o=dbzObj;
        }
        return o;


    }


    private boolean convertToBoolean(Object dbzObj) {
        if (dbzObj instanceof Boolean) {
            return (boolean) dbzObj;
        } else if (dbzObj instanceof Byte) {
            return (byte) dbzObj == 1;
        } else if (dbzObj instanceof Short) {
            return (short) dbzObj == 1;
        } else {
            return Boolean.parseBoolean(dbzObj.toString());
        }
    }

    private int convertToInt(Object dbzObj) {
        if (dbzObj instanceof Integer) {
            return (int) dbzObj;
        } else if (dbzObj instanceof Long) {
            return ((Long) dbzObj).intValue();
        } else {
            return Integer.parseInt(dbzObj.toString());
        }
    }

    private long convertToLong(Object dbzObj) {
        if (dbzObj instanceof Integer) {
            return (long) dbzObj;
        } else if (dbzObj instanceof Long) {
            return (long) dbzObj;
        } else {
            return Long.parseLong(dbzObj.toString());
        }
    }

    private double convertToDouble(Object dbzObj) {
        if (dbzObj instanceof Float) {
            return (double) dbzObj;
        } else if (dbzObj instanceof Double) {
            return (double) dbzObj;
        } else {
            return Double.parseDouble(dbzObj.toString());
        }
    }

    private float convertToFloat(Object dbzObj) {
        if (dbzObj instanceof Float) {
            return (float) dbzObj;
        } else if (dbzObj instanceof Double) {
            return ((Double) dbzObj).floatValue();
        } else {
            return Float.parseFloat(dbzObj.toString());
        }
    }

    private java.sql.Date convertToDate(Object dbzObj) {
        if (dbzObj != null) {
            String s = TemporalConversions.toLocalDate(dbzObj).toString();
            java.sql.Date parse = strToDate(s);
            return parse;

        }
        return null;
    }

    private int convertToTime(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Long) {
            switch (schema.name()) {
                case MicroTime.SCHEMA_NAME:
                    return (int) ((long) dbzObj / 1000);
                case NanoTime.SCHEMA_NAME:
                    return (int) ((long) dbzObj / 1000_000);

            }
        } else if (dbzObj instanceof Integer) {
            return (int) dbzObj;
        }
        // get number of milliseconds of the day
        return TemporalConversions.toLocalTime(dbzObj).toSecondOfDay() * 1000;
    }

    private java.sql.Timestamp  convertToTimestamp(Object dbzObj) {
        java.sql.Timestamp scurrtest = null;
        if (dbzObj instanceof Long) {
            scurrtest = new java.sql.Timestamp((long)dbzObj);

        }
        return scurrtest;
    }


    private StringData convertToString(Object dbzObj) {
        return StringData.fromString(dbzObj.toString());
    }

    private byte[] convertToBinary(Object dbzObj) {
        if (dbzObj instanceof byte[]) {
            return (byte[]) dbzObj;
        } else if (dbzObj instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) dbzObj;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return bytes;
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported BYTES value type: " + dbzObj.getClass().getSimpleName());
        }
    }




    /**
     * @param
     * */
    private static java.sql.Date strToDate(String strDate) {
        String str = strDate;

        Date d = null;
        try {
            d = format.parse(str);
        } catch (Exception e) {
            e.printStackTrace();
        }
        java.sql.Date date = new java.sql.Date(d.getTime());
        return date;
    }


    private static java.sql.Time strToTime(String strDate) {
        String str = strDate;
        SimpleDateFormat formathms = new SimpleDateFormat("HH:mm:ss");
        Date d = null;
        try {
            d = formathms.parse(str);
        } catch (Exception e) {
            e.printStackTrace();
        }
        java.sql.Time time = new java.sql.Time(d.getTime());
        return time.valueOf(str);
    }


}
