package com.nott.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
public class MigrationUtils {

    private static final Map<String, String> MYSQL_TO_POSTGRES_TYPE_MAP;

    static {
        MYSQL_TO_POSTGRES_TYPE_MAP = new HashMap<>();
        MYSQL_TO_POSTGRES_TYPE_MAP.put("BIT", "BIT");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("TINYINT", "SMALLINT");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("TINYINT UNSIGNED", "SMALLINT");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("SMALLINT", "SMALLINT");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("SMALLINT UNSIGNED", "INTEGER");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("MEDIUMINT", "INTEGER");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("MEDIUMINT UNSIGNED", "BIGINT");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("INT", "INTEGER");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("INT UNSIGNED", "BIGINT");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("BIGINT", "BIGINT");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("BIGINT UNSIGNED", "NUMERIC(20)");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("FLOAT", "REAL");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("DOUBLE", "DOUBLE PRECISION");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("DECIMAL", "DECIMAL");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("DATE", "DATE");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("TIME", "TIME");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("DATETIME", "TIMESTAMP");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("YEAR", "SMALLINT");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("CHAR", "CHAR");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("VARCHAR", "VARCHAR");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("BINARY", "BYTEA");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("VARBINARY", "BYTEA");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("TINYBLOB", "BYTEA");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("BLOB", "BYTEA");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("MEDIUMBLOB", "BYTEA");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("LONGBLOB", "BYTEA");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("TINYTEXT", "TEXT");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("MEDIUMTEXT", "TEXT");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("LONGTEXT", "TEXT");
        MYSQL_TO_POSTGRES_TYPE_MAP.put("TEXT", "TEXT");
    }

    public static String getPrimaryKeyByTableName(Connection conn, String tableNamePattern) throws SQLException {

        DatabaseMetaData dbMetaData = conn.getMetaData();

        ResultSet tabs = dbMetaData.getTables(null, null, tableNamePattern, new String[]{"TABLE"});

        List<String> pkColList = new ArrayList<>();

        while (tabs.next()) {
            ResultSet resultSet = dbMetaData.getPrimaryKeys(null, tabs.getString("TABLE_SCHEM"),
                    tabs.getString("TABLE_NAME"));

            while (resultSet.next()) {
                pkColList.add(resultSet.getString("COLUMN_NAME"));
            }
        }
        conn.close();

        return pkColList.stream().collect(Collectors.joining(","));
    }

    public static String[] getColumnsByTableName(Connection conn, String tableNamePattern) throws SQLException {

        DatabaseMetaData dbMetaData = conn.getMetaData();

        ResultSet tabs = dbMetaData.getTables(null, null, tableNamePattern, new String[]{"TABLE"});

        List<String> columnList = new ArrayList<>();

        while (tabs.next()) {
            ResultSet resultSet = dbMetaData.getColumns(null, tabs.getString("TABLE_SCHEM"),
                    tabs.getString("TABLE_NAME"), null);
            while (resultSet.next()) {
                columnList.add(resultSet.getString("COLUMN_NAME"));
            }
        }
        conn.close();
        return columnList.toArray(new String[columnList.size()]);
    }

    public static void createTableBySourceTabName(Connection mysqlConn, Connection postgresConn, String tableName) throws Exception {
        Assert.isTrue(mysqlConn != null,
                "source con is nul ");
        Assert.isTrue(postgresConn != null,
                "target con is nul");
        String createSql = generateCreateTableStatement(tableName, mysqlConn);
        try (PreparedStatement createTableStmt = postgresConn.prepareStatement(createSql)) {
            createTableStmt.executeUpdate();
            log.info("Table " + tableName + " created successfully.");
        } catch (SQLException ex) {
            log.info("Failed to create table " + tableName);

        }
        //mysqlConn.close();
    }

    private static String generateCreateTableStatement(String tableName, Connection conn) throws SQLException {
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        sb.append(tableName).append(" (");

        // Get column names and data types from MySQL database
        DatabaseMetaData metadata = conn.getMetaData();
        ResultSet rs = metadata.getColumns(conn.getCatalog(), null, tableName, null);
        while (rs.next()) {
            // MySQL column name and type
            String columnName = rs.getString("COLUMN_NAME");
            String dataType = rs.getString("TYPE_NAME");
            boolean isAutoIncrement = rs.getBoolean("IS_AUTOINCREMENT");
            if (isAutoIncrement) {
                dataType = "SERIAL";
            }
            // PostgreSQL data type mapping
            String postgresDataType = mapMySqlTypeToPostgres(dataType);

            sb.append(columnName).append(" ").append(postgresDataType).append(", ");
        }
        sb.setLength(sb.length() - 2);  // Remove trailing comma

        // Set primary key constraint
        ResultSet pkRs = metadata.getPrimaryKeys(null, null, tableName);
        if (pkRs.next()) {
            String pkColumnName = pkRs.getString("COLUMN_NAME");
            sb.append(", PRIMARY KEY (").append(pkColumnName).append(")");
        }

        sb.append(")");

        return sb.toString();
    }

    public static String mapMySqlTypeToPostgres(String mySqlType) {
        mySqlType = mySqlType.toUpperCase();
        String postgresType = MYSQL_TO_POSTGRES_TYPE_MAP.get(mySqlType);
        if (postgresType == null) {
            throw new IllegalArgumentException("Unsupported data type: " + mySqlType);
        }
        return postgresType;
    }


    public static void create() {
        log.info("run");
    }

    public static String getColumnDataTypeSql() {
        return "SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_NAME= ? AND COLUMN_NAME= ?";
    }


    public static void fillTableField(String tabName, DataSource source, DataSource target) throws Exception {
        String[] sourceColumns = getColumnsByTableName(source.getConnection(), tabName);
        String[] targetColumns = getColumnsByTableName(target.getConnection(), tabName);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(target);
        JdbcTemplate sjdbcTemplate = new JdbcTemplate(source);
        for (String sourceColumn : sourceColumns) {
            sourceColumn = sourceColumn.toLowerCase();
            boolean isContains = Arrays.asList(targetColumns).contains(sourceColumn);
            if (!isContains) {
                String columnDataTypeSql = getColumnDataTypeSql();
                log.info("query sql:{},{},{}", columnDataTypeSql,tabName,sourceColumn);
                String mysqlDataType = sjdbcTemplate.queryForList(columnDataTypeSql, new Object[]{tabName, sourceColumn}, String.class).get(0);
                String sqlTypeToPostgres = mapMySqlTypeToPostgres(mysqlDataType);
                log.info("table: [{}],missing field: [{}],mysqlType: [{}], newType:[{}]", tabName, sourceColumn, mysqlDataType, sqlTypeToPostgres);
                // fill field
                StringBuilder sqlBuilder = new StringBuilder(String.format("ALTER TABLE %s ADD COLUMN ", tabName));
                sqlBuilder.append(sourceColumn).append(" ").append(sqlTypeToPostgres);

                //if (options != null) {
                //    sqlBuilder.append(" ").append(options);
                //}

                String sql = sqlBuilder.toString();
                log.info("update sql:{}", sql);
                try {
                    jdbcTemplate.update(sql);
                } catch (BadSqlGrammarException e) {
                    if(e.getMessage().contains("already exists")){
                        log.info("field already exists");
                    }
                }
            }
        }
    }
}