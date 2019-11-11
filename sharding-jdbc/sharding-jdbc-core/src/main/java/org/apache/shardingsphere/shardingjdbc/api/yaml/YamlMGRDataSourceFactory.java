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

package org.apache.shardingsphere.shardingjdbc.api.yaml;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.core.yaml.config.mgr.YamlRootMGRConfiguration;
import org.apache.shardingsphere.core.yaml.engine.YamlEngine;
import org.apache.shardingsphere.core.yaml.swapper.impl.MGRRuleConfigurationYamlSwapper;
import org.apache.shardingsphere.shardingjdbc.api.MGRDataSourceFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

/**
 * MGR data source factory for YAML.
 *
 * @author zhangyonglun
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class YamlMGRDataSourceFactory {
    
    /**
     * Create MGR data source.
     *
     * @param yamlFile YAML file for MGR rule configuration with data sources
     * @return MGR data source
     * @throws SQLException SQL exception
     * @throws IOException IO exception
     */
    public static DataSource createDataSource(final File yamlFile) throws SQLException, IOException {
        YamlRootMGRConfiguration config = YamlEngine.unmarshal(yamlFile, YamlRootMGRConfiguration.class);
        return MGRDataSourceFactory.createDataSource(config.getDataSources(), new MGRRuleConfigurationYamlSwapper().swap(config.getMgrRule()), config.getProps());
    }
    
    /**
     * Create MGR data source.
     *
     * @param yamlBytes YAML bytes for MGR rule configuration with data sources
     * @return MGR data source
     * @throws SQLException SQL exception
     * @throws IOException IO exception
     */
    public static DataSource createDataSource(final byte[] yamlBytes) throws SQLException, IOException {
        YamlRootMGRConfiguration config = YamlEngine.unmarshal(yamlBytes, YamlRootMGRConfiguration.class);
        return MGRDataSourceFactory.createDataSource(config.getDataSources(), new MGRRuleConfigurationYamlSwapper().swap(config.getMgrRule()), config.getProps());
    }
    
    /**
     * Create MGR data source.
     *
     * @param dataSourceMap data source map
     * @param yamlFile YAML file for MGR rule configuration without data sources
     * @return MGR data source
     * @throws SQLException SQL exception
     * @throws IOException IO exception
     */
    public static DataSource createDataSource(final Map<String, DataSource> dataSourceMap, final File yamlFile) throws SQLException, IOException {
        YamlRootMGRConfiguration config = YamlEngine.unmarshal(yamlFile, YamlRootMGRConfiguration.class);
        return MGRDataSourceFactory.createDataSource(dataSourceMap, new MGRRuleConfigurationYamlSwapper().swap(config.getMgrRule()), config.getProps());
    }
    
    /**
     * Create MGR data source.
     *
     * @param dataSourceMap data source map
     * @param yamlBytes YAML bytes for MGR rule configuration without data sources
     * @return MGR data source
     * @throws SQLException SQL exception
     * @throws IOException IO exception
     */
    public static DataSource createDataSource(final Map<String, DataSource> dataSourceMap, final byte[] yamlBytes) throws SQLException, IOException {
        YamlRootMGRConfiguration config = YamlEngine.unmarshal(yamlBytes, YamlRootMGRConfiguration.class);
        return MGRDataSourceFactory.createDataSource(dataSourceMap, new MGRRuleConfigurationYamlSwapper().swap(config.getMgrRule()), config.getProps());
    }
}
