package com.xuanwu.source.mysql;

import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;

/**
 * @description:
 * @author: lugela
 * @create: 2022-08-28 15:06
 */

public class MysqlSourceConfig implements Serializable {
    private static final String HOSTNAME = "hostname";
    private static final String PORT = "port";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String DATABASE_NAME = "database-name";
    private static final String TABLE_NAME = "table-name";
    private static final String SERVER_TIME_ZONE = "server-time-zone";
    private static final String SERVER_ID = "server-id";
    private static final String SCAN_INCREMENTAL_SNAPSHOT_ENABLED = "scan.incremental.snapshot.enabled";
    private static final String SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE = "scan.incremental.snapshot.chunk.size";
    private static final String SCAN_SNAPSHOT_FETCH_SIZE = "scan.snapshot.fetch.size";
    private static final String CONNECT_TIMEOUT = "connect.timeout";
    private static final String CONNECTION_POOL_SIZE = "connection.pool.size";
    private static final String CONNECT_MAX_RETRIES = "connect.max-retries";
    private static final String SCAN_STARTUP_MODE = "scan.startup.mode";
    private static final String SCAN_STARTUP_SPECIFIC_OFFSET_FILE = "scan.startup.specific-offset.file";
    private static final String SCAN_STARTUP_SPECIFIC_OFFSET_POS = "scan.startup.specific-offset.pos";
    private static final String SCAN_STARTUP_TIMESTAMP_MILLIS = "scan.startup.timestamp-millis";
    private static final String HEARTBEAT_INTERVAL = "heartbeat.interval";
    private static final String PREFIX_JDBC_PROPERTIES ="jdbc.properties.";
    private static final String PREFIX_DEBEZIUM = "debezium.";
    private static final String STARTUP_DATE_MODE = "start.date.mode";
    private static final String CHUNK_KEY_COLUMN = "chunk_key_column";

    public static MysqlSourceOptions buildMysqlSourceOptions(Config config) {

      MysqlSourceOptions mysqlSourceOptions = new MysqlSourceOptions();

        /*required*/
        if (config.hasPath(HOSTNAME)){
            mysqlSourceOptions.setHostname(config.getString(HOSTNAME));
        }

        if (config.hasPath(PORT)){
            mysqlSourceOptions.setPort(config.getInt(PORT));
        }

        if (config.hasPath(USERNAME)){
            mysqlSourceOptions.setUsername(config.getString(USERNAME));
        }

        if (config.hasPath(PASSWORD)){
            mysqlSourceOptions.setPassword(config.getString(PASSWORD));
        }

        if (config.hasPath(DATABASE_NAME)){
            mysqlSourceOptions.setDatabaseList(config.getString(DATABASE_NAME).split(","));
        }

        if (config.hasPath(TABLE_NAME)){
            mysqlSourceOptions.setTableList(config.getString(TABLE_NAME).split(","));
        }
        /*required*/


        /*optional*/
        if (config.hasPath(SERVER_TIME_ZONE)){
            mysqlSourceOptions.setServerTimeZone(config.getString(SERVER_TIME_ZONE).toUpperCase());
        }

        if (config.hasPath(SERVER_ID)){
            mysqlSourceOptions.setServerId(config.getString(SERVER_ID));
        }

        if (config.hasPath(SCAN_STARTUP_MODE)){
            String scanStartupMode = config.getString(SCAN_STARTUP_MODE).toUpperCase();
            switch(scanStartupMode) {
                case "INITIAL":
                    mysqlSourceOptions.setStartupOptions(StartupOptions.initial());
                    break;
                case "EARLIEST_OFFSET":
                    mysqlSourceOptions.setStartupOptions(StartupOptions.earliest());
                    break;
                case "LATEST_OFFSET":
                    mysqlSourceOptions.setStartupOptions(StartupOptions.latest());
                    break;
                case "SPECIFIC_OFFSETS":
                    //StartupOptions.specificOffset();
                    break;
                case "TIMESTAMP":
                    String start_date = config.getString(STARTUP_DATE_MODE);
                    SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    try {
                        Date parse = format2.parse(start_date);
                        mysqlSourceOptions.setStartupOptions(StartupOptions.timestamp(parse.getTime()));
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(SCAN_STARTUP_MODE + " mode is not supported.");
            }

        }

        if (config.hasPath(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE)){
            mysqlSourceOptions.setSplitSize(config.getInt(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE));
        }

        if (config.hasPath(SCAN_SNAPSHOT_FETCH_SIZE)){
            mysqlSourceOptions.setFetchSize(config.getInt(SCAN_SNAPSHOT_FETCH_SIZE));
        }

        if (config.hasPath(CONNECT_TIMEOUT)){
            mysqlSourceOptions.setConnectTimeout(Duration.ofSeconds(config.getLong(CONNECT_TIMEOUT)));
        }

        if (config.hasPath(CONNECTION_POOL_SIZE)){
            mysqlSourceOptions.setConnectionPoolSize(config.getInt(CONNECTION_POOL_SIZE));
        }

        if (config.hasPath(CONNECT_MAX_RETRIES)){
            mysqlSourceOptions.setConnectMaxRetries(config.getInt(CONNECT_MAX_RETRIES));
        }


        if (config.hasPath(CONNECT_MAX_RETRIES)){
            mysqlSourceOptions.setConnectMaxRetries(config.getInt(CONNECT_MAX_RETRIES));
        }


        if (config.hasPath(CONNECT_MAX_RETRIES)){
            mysqlSourceOptions.setConnectMaxRetries(config.getInt(CONNECT_MAX_RETRIES));
        }


        if (config.hasPath(CHUNK_KEY_COLUMN)){
            mysqlSourceOptions.setChunkKeyColumn(config.getString(CHUNK_KEY_COLUMN));
        }
        Properties jdbcproperties = new Properties();
        Properties debeproperties = new Properties();

        config.entrySet().forEach(x -> {
            String key = x.getKey();
            if (key.contains(PREFIX_JDBC_PROPERTIES)){
                jdbcproperties.setProperty(x.getKey(),config.getString(x.getKey()));
            }

            if (key.contains(PREFIX_DEBEZIUM)){
                debeproperties.setProperty(x.getKey(),config.getString(x.getKey()));
            }

        });

        mysqlSourceOptions.setDbzProperties(debeproperties);
        mysqlSourceOptions.setJdbcProperties(jdbcproperties);

        return mysqlSourceOptions;

    }
}
