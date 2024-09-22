package com.alibaba.otter.node.extend.processor;

import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;
import com.google.common.collect.Lists;
import junit.framework.TestCase;

/**
 * @author kqwu@best-inc.com
 * @description
 * @date 2024/9/22
 */
public class OgnlEventProcessorTest extends TestCase {

    OgnlEventProcessor eventProcessor;

    public void setUp() throws Exception {
        eventProcessor = new OgnlEventProcessor("#event.column(\"customer_id\") == 10");
        super.setUp();
    }

    public void tearDown() throws Exception {
    }

    public void testProcess() {
        final EventData eventData = new EventData();
        EventColumn eventColumn = new EventColumn();
        eventColumn.setColumnName("customer_id");
        eventColumn.setColumnValue("10");
        eventData.setColumns(Lists.newArrayList(eventColumn));
        boolean result = eventProcessor.process(eventData);
        assertTrue(result);
        eventColumn.setColumnName("customer_id");
        eventColumn.setColumnValue("1");
        eventData.setColumns(Lists.newArrayList(eventColumn));
        result = eventProcessor.process(eventData);
        assertFalse(result);
        eventColumn.setColumnName("id");
        eventColumn.setColumnValue("10");
        eventData.setColumns(Lists.newArrayList(eventColumn));
        result = eventProcessor.process(eventData);
        assertFalse(result);
    }
}