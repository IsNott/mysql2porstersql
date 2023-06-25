package com.nott;

import com.nott.utils.MigrationUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Nott
 * @Date 2023/6/2
 */

@Slf4j
public class DataTableMigration extends CommonMigration {

    private final JdbcTemplate targetJdbc;
    private final JdbcTemplate sourceJdbc;
    private final String tableName;
    private final String primaryKey;
    private final String[] columnNamesInSourceDB;
    private final String[] columnNamesInTargetDB;

    private final Map<String, String> columnMappings;

    public DataTableMigration(DataSource sourceDataSource, String tableName, DataSource targetDataSource) throws SQLException {
        this(sourceDataSource, targetDataSource, tableName, new HashMap<>());
    }

    public DataTableMigration(DataSource sourceDataSource, DataSource targetDataSource, String tableName,
                              Map<String, String> columnMappings)
            throws SQLException {
        this.tableName = tableName.toLowerCase();
        this.sourceJdbc = new JdbcTemplate(sourceDataSource);
        this.targetJdbc = new JdbcTemplate(targetDataSource);
        this.primaryKey = MigrationUtils.getPrimaryKeyByTableName(sourceDataSource.getConnection(), this.tableName);
        this.columnNamesInSourceDB = MigrationUtils.getColumnsByTableName(sourceDataSource.getConnection(), this.tableName);
        Assert.isTrue(this.columnNamesInSourceDB != null && this.columnNamesInSourceDB.length > 0,
                "can't find column infor from source db for the table " + this.tableName);
        this.columnNamesInTargetDB = MigrationUtils.getColumnsByTableName(targetDataSource.getConnection(), this.tableName);
        Assert.isTrue(this.columnNamesInTargetDB != null && this.columnNamesInTargetDB.length > 0,
                "can't find column infor from target db for the table " + this.tableName);
        this.columnMappings = columnMappings;
    }

    protected JdbcTemplate getSourceJdbc() {
        return this.sourceJdbc;
    }

    protected JdbcTemplate getTargetJdbc() {
        return this.targetJdbc;
    }

    @Override
    protected void closeConnect() {

    }

    @Override
    protected List<Map<String, Object>> queryForList(String querySql, int offset, int stepLength) {
        List<Map<String, Object>> queryForList = new ArrayList<>();
        try {
            queryForList = getSourceJdbc().queryForList(querySql, offset, stepLength);
        } catch (DataAccessException e) {
            log.error("query table {} data error:{},break",this.tableName,e.getMessage());
        }
        return queryForList;
    }

    @Override
    protected void batchInsert(List<Map<String, Object>> rows) throws SQLException {
        String insertSQL = getInsertSQL();
        log.info("insert sql: {}",insertSQL);
        getTargetJdbc().batchUpdate(insertSQL,
                rows.stream().map(this::rowToParam)
                        .collect(Collectors.toList()));

    }

    private Object[] rowToParam(Map<String, Object> row) {
        return Arrays.stream(columnNamesInTargetDB)
                .map(colInSource -> columnMappings.getOrDefault(colInSource, colInSource))
                .map(row::get)
                .toArray();
    }

    protected String getInsertSQL() {
        return String.format("insert into %s (%s) values(%s)",
                this.tableName,
                String.join(",", columnNamesInTargetDB),
                IntStream.range(0, columnNamesInTargetDB.length)
                        .mapToObj(n -> "?")
                        .collect(Collectors.joining(",")));
    }

    @Override
    protected String getQuerySql() {

        //return String.format("select %s"
        //                + " from %s"
        //                + "    order by %s asc "
        //                + "    limit ?, ?",
        //        String.join(",", columnNamesInSourceDB),
        //        this.tableName,
        //        this.primaryKey);
        return String.format("select * from %s limit ?,?",this.tableName);
    }

    @Override
    protected int getStepLength() {
        return 100;
    }

    @Override
    protected int getTotalRecords() {
        int count = getSourceJdbc().queryForObject(
                "select count(1) from " + tableName, Integer.class);
        return count;
    }


}
