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

package org.apache.shardingsphere.shardingjdbc.jdbc.core.datasource;

import lombok.Getter;
import org.apache.shardingsphere.core.rule.MGRRule;
import org.apache.shardingsphere.shardingjdbc.jdbc.adapter.AbstractDataSourceAdapter;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.connection.MGRConnection;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.context.MGRRuntimeContext;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * MGR data source.
 *
 * @author zhangyonglun
 */
@Getter
public class MGRDataSource extends AbstractDataSourceAdapter {
    
    private final MGRRuntimeContext runtimeContext;
    
    public MGRDataSource(final Map<String, DataSource> dataSourceMap, final MGRRule mgrRule, final Properties props) throws SQLException {
        super(dataSourceMap);
        runtimeContext = new MGRRuntimeContext(dataSourceMap, mgrRule, props, getDatabaseType());
    }
    
    @Override
    public final MGRConnection getConnection() {
        return new MGRConnection(getDataSourceMap(), runtimeContext);
    }
    
    @Override
    public final void close() throws Exception {
        super.close();
        runtimeContext.close();
    }
    
}
