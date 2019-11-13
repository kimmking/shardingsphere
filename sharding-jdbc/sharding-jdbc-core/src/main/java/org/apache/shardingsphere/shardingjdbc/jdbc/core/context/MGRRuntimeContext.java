/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.shardingjdbc.jdbc.core.context;

import lombok.Getter;
import org.apache.shardingsphere.core.exception.ShardingException;
import org.apache.shardingsphere.core.rule.MGRRule;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.datasource.metadata.CachedDatabaseMetaData;
import org.apache.shardingsphere.spi.database.DatabaseType;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runtime context for MGR.
 *
 * @author zhangyonglun
 */
@Getter
public final class MGRRuntimeContext extends AbstractRuntimeContext<MGRRule> {
    
    private static final String PLUGIN_STATUS = "SELECT * FROM information_schema.PLUGINS WHERE PLUGIN_NAME='group_replication'";
    
    private static final String MEMBER_COUNT = "SELECT count(*) FROM performance_schema.replication_group_members";
    
    private static final String GROUP_NAME = "SELECT * FROM performance_schema.global_variables WHERE VARIABLE_NAME='group_replication_group_name'";
    
    private static final String SINGLE_PRIMARY = "SELECT * FROM performance_schema.global_variables WHERE VARIABLE_NAME='group_replication_single_primary_mode'";
    
    private DatabaseMetaData cachedDatabaseMetaData;
    
    private String primaryDataSource;
    
    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    
    public MGRRuntimeContext(final Map<String, DataSource> dataSourceMap, final MGRRule rule, final Properties props, final DatabaseType databaseType) throws SQLException {
        super(rule, props, databaseType);
        try {
            updatePrimaryDataSource(dataSourceMap);
            checkMGRConfig(dataSourceMap);
            periodicalMonitor(dataSourceMap);
            cachedDatabaseMetaData = createCachedDatabaseMetaData(dataSourceMap);
        } catch (final SQLException ex) {
            ex.printStackTrace();
        }
    }
    
    private DatabaseMetaData createCachedDatabaseMetaData(final Map<String, DataSource> dataSourceMap) throws SQLException {
        try (Connection connection = dataSourceMap.get(primaryDataSource).getConnection()) {
            return new CachedDatabaseMetaData(connection.getMetaData(), dataSourceMap, null);
        }
    }
    
    /**
     * Update primary DataSource.
     *
     * @param dataSourceMap DataSource map
     */
    public void updatePrimaryDataSource(final Map<String, DataSource> dataSourceMap) {
        String primary = queryPrimaryDataSource(dataSourceMap);
        if (!"".equals(primary)) {
            primaryDataSource = primary;
        }
    }
    
    /**
     * Check MGR config.
     *
     * @param dataSourceMap DataSource map
     * @throws SQLException SQL Exception
     */
    public void checkMGRConfig(final Map<String, DataSource> dataSourceMap) throws SQLException {
        try (Connection connection = dataSourceMap.get(primaryDataSource).getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(PLUGIN_STATUS);
            while (resultSet.next()) {
                if (!"ACTIVE".equals(resultSet.getString("PLUGIN_STATUS"))) {
                    throw new ShardingException("MGR plugin is not active.");
                }
            }
            resultSet.close();
            resultSet = statement.executeQuery(MEMBER_COUNT);
            while (resultSet.next()) {
                if (Integer.parseInt(resultSet.getString(1)) < 1) {
                    throw new ShardingException("MGR member count < 1");
                }
            }
            resultSet.close();
            resultSet = statement.executeQuery(GROUP_NAME);
            while (resultSet.next()) {
                String serverGroupName = resultSet.getString("VARIABLE_VALUE");
                String ruleGroupName = getRule().getGroupName();
                if (!serverGroupName.equals(ruleGroupName)) {
                    throw new ShardingException("MGR group name is not consistent\n" + "serverGroupName: " + serverGroupName
                        + "\nruleGroupName: " + ruleGroupName);
                }
            }
            resultSet.close();
            resultSet = statement.executeQuery(SINGLE_PRIMARY);
            while (resultSet.next()) {
                if (!"ON".equals(resultSet.getString("VARIABLE_VALUE"))) {
                    throw new ShardingException("MGR is not in single primary mode");
                }
            }
            resultSet.close();
        }
    }
    
    private String queryPrimaryDataSource(final Map<String, DataSource> dataSourceMap) {
        String result = "";
        String urlResult = "";
        for (Map.Entry<String, DataSource> entry : dataSourceMap.entrySet()) {
            DataSource dataSource = entry.getValue();
            String url = "";
            String sql = "SELECT MEMBER_HOST, MEMBER_PORT FROM performance_schema.replication_group_members WHERE MEMBER_ID = "
                + "(SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'group_replication_primary_member')";
            try (Connection connection = dataSource.getConnection();
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    url = resultSet.getString("MEMBER_HOST");
                    url += ":";
                    url += resultSet.getString("MEMBER_PORT");
                }
                // CHECKSTYLE:OFF
            } catch (final Exception ex) {
                // CHECKSTYLE:ON
            }
            if (null != url && !"".equals(url) && !"".equals(urlResult) && !urlResult.equals(url)) {
                return result;
            }
            urlResult = url;
        }
        for (Map.Entry<String, DataSource> entry : dataSourceMap.entrySet()) {
            DataSource dataSource = entry.getValue();
            try (Connection connection = dataSource.getConnection()) {
                if (connection.getMetaData().getURL().contains(urlResult)) {
                    result = entry.getKey();
                    break;
                }
                // CHECKSTYLE:OFF
            } catch (final Exception ex) {
                // CHECKSTYLE:ON
            }
        }
        return result;
    }
    
    private void periodicalMonitor(final Map<String, DataSource> dataSourceMap) {
        Runnable runnable = new Runnable() {
            public void run() {
                updatePrimaryDataSource(dataSourceMap);
            }
        };
        scheduledExecutorService.scheduleAtFixedRate(runnable, 0, Integer.parseInt(getRule().getKeepAliveSeconds()), TimeUnit.SECONDS);
    }
    
    @Override
    public void close() throws Exception {
        try {
            scheduledExecutorService.shutdown();
            while (!scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduledExecutorService.shutdownNow();
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        super.close();
    }
}
