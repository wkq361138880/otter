package com.alibaba.otter.node.extend.processor;

import com.alibaba.otter.shared.etl.model.EventData;

import java.util.Objects;

public class FilterEventProcessor extends AbstractEventProcessor {
    @Override
    public boolean process(EventData eventData) {
        return Objects.equals(getColumn(eventData, "customer_id").getColumnValue(), "");
    }
}
