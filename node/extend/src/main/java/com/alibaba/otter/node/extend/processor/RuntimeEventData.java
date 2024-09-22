package com.alibaba.otter.node.extend.processor;

import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;
import org.apache.commons.lang.StringUtils;

/**
 * @author kqwu@best-inc.com
 * @description
 * @date 2024/9/22
 */
public class RuntimeEventData extends EventData {

    RuntimeEventData(EventData eventData) {
        setColumns(eventData.getColumns());
        setEventType(eventData.getEventType());
        setDdlSchemaName(eventData.getDdlSchemaName());
        setOldKeys(eventData.getOldKeys());
        setTableName(eventData.getTableName());
        setTableId(eventData.getTableId());
    }

    public String column(String columnName) {
        if (getColumns().size() == 0 || columnName == null) {
            return null;
        }
        for (EventColumn column : getColumns()) {
            if (StringUtils.equalsIgnoreCase(column.getColumnName(), columnName)) {
                return column.getColumnValue();
            }
        }
        for (EventColumn column : getKeys()) {
            if (StringUtils.equalsIgnoreCase(column.getColumnName(), columnName)) {
                return column.getColumnValue();
            }
        }
        return null;
    }
}
