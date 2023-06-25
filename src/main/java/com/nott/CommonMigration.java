package com.nott;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;

import java.util.List;
import java.util.Map;

/**
 * @author Nott
 * @Date 2023/6/2
 */


abstract public class CommonMigration {

    private static Logger LOG = LoggerFactory.getLogger(CommonMigration.class);

    public void migrate(String sourceTab, String targetTab) throws Exception {

        int totalRecords = getTotalRecords();

        int stepLength = getStepLength();
        LOG.info("start to migrate data from source db to target db");
        LOG.info("source table:{}", sourceTab);
        LOG.info("target table:{}", targetTab);
        String querySql = getQuerySql();
        LOG.info("starting to query,sql:{}", querySql);
        for (int offset = 0; offset < totalRecords; offset = offset + stepLength) {
            List<Map<String, Object>> rows = queryForList(querySql, offset, stepLength);
            // 说明发生错误
            if (rows.isEmpty()) {
                break;
            }
            int count = offset;
            try {
                batchInsert(rows);
            } catch (DuplicateKeyException e) {
                count--;
                LOG.info("duplicate key,continue to insert");
            }
            LOG.info("moved {} records", count);
        }
    }

    protected abstract void closeConnect();

    abstract protected List<Map<String, Object>> queryForList(String querySql, int offset, int stepLength);

    abstract protected String getQuerySql();

    abstract protected void batchInsert(List<Map<String, Object>> collocMaps) throws Exception;

    protected int getStepLength() {
        return 100;
    }

    protected int getInitialOffset() {
        return 0;
    }

    abstract protected int getTotalRecords();
}
