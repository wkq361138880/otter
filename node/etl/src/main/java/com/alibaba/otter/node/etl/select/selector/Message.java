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

package com.alibaba.otter.node.etl.select.selector;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.node.etl.select.exceptions.SelectException;
import com.alibaba.otter.shared.etl.model.EventData;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据对象
 *
 * @author jianghang 2012-7-31 下午02:43:08
 */
public class Message {

    private Long id;
    private List<EventData> datas;

    private com.alibaba.otter.canal.protocol.Message originalMessage;

    public Message(Long id, List<EventData> datas) {
        this.id = id;
        this.datas = datas;
    }

    public Message(com.alibaba.otter.canal.protocol.Message originalMessage) {
        this.originalMessage = originalMessage;
        this.id = originalMessage.getId();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public List<EventData> getDatas() {
        return datas;
    }

    public void setDatas(List<EventData> datas) {
        this.datas = datas;
    }

    public boolean hasData() {
        if (originalMessage.isRaw()) {
            return originalMessage.getRawEntries().size() > 0;
        } else {
            return originalMessage.getEntries().size() > 0;
        }
    }

    public com.alibaba.otter.canal.protocol.Message getOriginalMessage() {
        return originalMessage;
    }
}
