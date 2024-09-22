/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.otter.node.etl.load.loader.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.otter.shared.etl.model.EventData;
import com.alibaba.otter.shared.etl.model.EventType;
import com.google.common.collect.Lists;

/**
 * 将同一个weight下的EventData进行数据归类,按表和insert/update/delete类型进行分类
 *
 * <pre>
 * 归类用途：对insert语句进行batch优化
 * 1. mysql索引的限制，需要避免insert并发执行
 * </pre>
 *
 * @author jianghang 2011-11-9 下午04:28:35
 * @version 4.0.0
 */
public class DbLoadData {

    private Map<Long,TableLoadData> tableMap = new HashMap<>();

    public DbLoadData(){
        // nothing
    }

    public boolean merge(EventData data) {
        TableLoadData tableData = findTableData(data.getTableId());

        EventType type = data.getEventType();
        if (type.isInsert()) {
            tableData.getInsertDatas().add(data);
        } else if (type.isUpdate()) {
            tableData.getUpadateDatas().add(data);
        } else if (type.isDelete()) {
            tableData.getDeleteDatas().add(data);
        } else {
            return false;
        }
        return true;
    }

    public List<TableLoadData> getTables() {
        return Lists.newArrayList(tableMap.values());
    }

    private synchronized TableLoadData findTableData(Long tableId) {
        TableLoadData data = tableMap.get(tableId);
        if(data == null){
            data = new TableLoadData(tableId);
            tableMap.put(tableId,data);
        }
        return data;
    }

    /**
     * 按table进行分类
     */
    public static class TableLoadData {

        private Long            tableId;
        private List<EventData> insertDatas  = new ArrayList<EventData>();
        private List<EventData> upadateDatas = new ArrayList<EventData>();
        private List<EventData> deleteDatas  = new ArrayList<EventData>();

        public TableLoadData(Long tableId){
            this.tableId = tableId;
        }

        public List<EventData> getInsertDatas() {
            return insertDatas;
        }

        public void setInsertDatas(List<EventData> insertDatas) {
            this.insertDatas = insertDatas;
        }

        public List<EventData> getUpadateDatas() {
            return upadateDatas;
        }

        public void setUpadateDatas(List<EventData> upadateDatas) {
            this.upadateDatas = upadateDatas;
        }

        public List<EventData> getDeleteDatas() {
            return deleteDatas;
        }

        public void setDeleteDatas(List<EventData> deleteDatas) {
            this.deleteDatas = deleteDatas;
        }

        public Long getTableId() {
            return tableId;
        }

        public void setTableId(Long tableId) {
            this.tableId = tableId;
        }
    }
}
