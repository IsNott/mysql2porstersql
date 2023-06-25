package com.nott.controller;


import com.alibaba.fastjson.JSONObject;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.nott.DataTableMigration;
import com.nott.utils.MigrationUtils;
import com.nott.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Nott
 * @Date 2023/6/2
 */

@RestController
@RequestMapping("m2p")
@Slf4j
public class Controller {

    @Resource
    private Config config;

    @RequestMapping("/create")
    @Transactional(rollbackFor = Exception.class)
    public void create(@RequestBody JSONObject jsonObject) {
        boolean isNeedData = jsonObject.getBoolean("data");
        Connection sourceConnection = null;
        Connection targetConn = null;
        try {
            DataSource source = config.sourceDataSource();
            DataSource targetDatasource = config.targetDatasource();
            sourceConnection = source.getConnection();
            targetConn = targetDatasource.getConnection();
            List<String> sourceTableNames = getTableNamesByConn(sourceConnection);
            List<String> targetTableNames = getTableNamesByConn(targetConn);

            if (sourceTableNames.isEmpty()) {
                return;
            }

            // create table
            for (String tableName : sourceTableNames) {
                if (!targetTableNames.contains(tableName)) {
                    MigrationUtils.createTableBySourceTabName(sourceConnection, targetConn, tableName);
                }
            }
           if(isNeedData){
               // mysql data 2 pgsql
               for (String tableName : sourceTableNames) {
                   new DataTableMigration(source, tableName, targetDatasource).migrate(tableName, tableName);
               }
           }
            log.info("move data done..");
        } catch (Exception e) {
            log.error("error : {}", e.getMessage(), e);
        } finally {
            try {
                if (sourceConnection != null) {
                    sourceConnection.close();
                }
                if (targetConn != null) {
                    targetConn.close();
                }
                log.info("end..");
            } catch (Exception e) {
                log.error("error : {}", e.getMessage(), e);
            }
        }
    }

    private List<String> getTableNamesByConn(Connection conn) throws Exception {
        if (conn == null) {
            throw new RuntimeException("conn is null");
        }
        // Get all table names
        String[] types = {"TABLE"};
        ResultSet resultSet = conn.getMetaData().getTables(conn.getCatalog(), null, "%", types);
        List<String> tabNams = new ArrayList<>();
        while (resultSet.next()) {
            try {
                String tableName = resultSet.getString("TABLE_NAME");
                if (StringUtils.isNotEmpty(tableName)) {
                    tabNams.add(tableName);
                }
            } catch (SQLException e) {
                if (e.getMessage().contains("After end of result set")) {
                    break;
                }
            }
        }

        return tabNams;
    }

    @RequestMapping("fill")
    @Transactional(rollbackFor = Exception.class)
    public void file() {
        Connection sourceConnection = null;
        Connection targetConn = null;
        try {
            DataSource source = config.sourceDataSource();
            DataSource targetDatasource = config.targetDatasource();
            sourceConnection = source.getConnection();
            targetConn = targetDatasource.getConnection();
            List<String> sourceTableNames = getTableNamesByConn(sourceConnection);
            List<String> targetTableNames = getTableNamesByConn(targetConn);

            if (sourceTableNames.isEmpty()) {
                return;
            }

            for (String sourceTable : sourceTableNames) {
                if (targetTableNames.contains(sourceTable)) {
                    String tabName = sourceTable;
                    MigrationUtils.fillTableField(tabName, source, targetDatasource);
                }
            }
        } catch (Exception e) {
            log.error("error : {}", e.getMessage(), e);
        } finally {
            try {
                if (sourceConnection != null) {
                    sourceConnection.close();
                }
                if (targetConn != null) {
                    targetConn.close();
                }
                log.info("end..");
            } catch (Exception e) {
                log.error("error : {}", e.getMessage(), e);
            }
        }
    }

}
