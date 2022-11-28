package com.xuanwu.source.oracle;

import com.ververica.cdc.connectors.oracle.table.StartupOptions;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import java.io.Serializable;


/**
 * @description:
 * @author: lugela
 * @create: 2022-08-28 15:06
 */

public class OracleSourceConfig implements Serializable {
    public static final String HOSTNAME = "hostname";
    public static final String PORT = "port";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String DATABASE_NAME = "database-name";
    public static final String TABLE_NAME = "table-name";
    public static final String SERVER_TIME_ZONE = "server-time-zone";
    public static final String SCAN_STARTUP_MODE = "scan.startup.mode";
    public static final String STARTUP_DATE_MODE = "start.date.mode";
    public static final String SCHEMA_NAME = "schema-name";
    public static final String URL = "url";

    public static OracleSourceOptions buildOracleSourceOptions(Config config) {

      OracleSourceOptions oracleSourceOptions = new OracleSourceOptions();

        /*required*/
        if (config.hasPath(HOSTNAME)){
            oracleSourceOptions.setHostname(config.getString(HOSTNAME));
        }

        if (config.hasPath(PORT)){
            oracleSourceOptions.setPort(config.getInt(PORT));
        }

        if (config.hasPath(USERNAME)){
            oracleSourceOptions.setUsername(config.getString(USERNAME));
        }

        if (config.hasPath(PASSWORD)){
            oracleSourceOptions.setPassword(config.getString(PASSWORD));
        }

        if (config.hasPath(DATABASE_NAME)){
            oracleSourceOptions.setDatabase(config.getString(DATABASE_NAME));
        }


        if (config.hasPath(URL)){
            oracleSourceOptions.setUrl(config.getString(URL));
        }



        if (config.hasPath(TABLE_NAME)){
            oracleSourceOptions.setTableList(config.getString(TABLE_NAME).split(","));
        }
        if (config.hasPath(SCHEMA_NAME)){
            oracleSourceOptions.setSchemaList(config.getString(SCHEMA_NAME).split(","));
        }

        /*required*/


        /*optional*/
        if (config.hasPath(SERVER_TIME_ZONE)){
            oracleSourceOptions.setServerTimeZone(config.getString(SERVER_TIME_ZONE).toUpperCase());
        }



        if (config.hasPath(SCAN_STARTUP_MODE)){
            String scanStartupMode = config.getString(SCAN_STARTUP_MODE).toUpperCase();
            switch(scanStartupMode) {
                case "INITIAL":
                    oracleSourceOptions.setStartupOptions(StartupOptions.initial());
                    break;
                case "LATEST_OFFSET":
                    oracleSourceOptions.setStartupOptions(StartupOptions.latest());
                    break;
                default:
                    throw new UnsupportedOperationException(SCAN_STARTUP_MODE + " mode is not supported.");
            }

        }
        return oracleSourceOptions;

    }
}
