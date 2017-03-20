//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.threads;

import com.microsoft.kafkaavailability.*;
import com.microsoft.kafkaavailability.properties.ConsumerProperties;
import com.microsoft.kafkaavailability.properties.MetaDataManagerProperties;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;

import static com.microsoft.kafkaavailability.discovery.Constants.DEFAULT_ELAPSED_TIME;

public class ConsumerPartitionThread implements Callable<Long> {

    final static Logger m_logger = LoggerFactory.getLogger(ConsumerPartitionThread.class);
    private CuratorFramework m_curatorFramework;
    private TopicMetadata m_TopicMetadata;
    private PartitionMetadata m_PartitionMetadata;

    public ConsumerPartitionThread(CuratorFramework curatorFramework, TopicMetadata topicMetadata, PartitionMetadata partitionMetadata) {
        this.m_curatorFramework = curatorFramework;
        this.m_TopicMetadata = topicMetadata;
        this.m_PartitionMetadata = partitionMetadata;
    }

    @Override
    public Long call() throws Exception {
        return Long.valueOf(RunConsumerPartitionThread());
    }

    private long RunConsumerPartitionThread() throws IOException, MetaDataManagerException {

        IPropertiesManager consumerPropertiesManager = new PropertiesManager<ConsumerProperties>("consumerProperties.json", ConsumerProperties.class);
        IPropertiesManager metaDataPropertiesManager = new PropertiesManager<MetaDataManagerProperties>("metadatamanagerProperties.json", MetaDataManagerProperties.class);
        IMetaDataManager metaDataManager = new MetaDataManager(m_curatorFramework, metaDataPropertiesManager);
        IConsumer consumer = new Consumer(consumerPropertiesManager, metaDataManager);

        long startTime, endTime;

        m_logger.debug("Reading from Topic: {}; Partition: {};", m_TopicMetadata.topic(), m_PartitionMetadata.partitionId());
        startTime = System.currentTimeMillis();
        try {
            consumer.ConsumeFromTopicPartition(m_TopicMetadata.topic(), m_PartitionMetadata.partitionId());
            endTime = System.currentTimeMillis();
        } catch (Exception e) {
            m_logger.error("Error Reading from Topic: {}; Partition: {}; Exception: {}", m_TopicMetadata.topic(), m_PartitionMetadata.partitionId(), e);
            endTime = System.currentTimeMillis() + DEFAULT_ELAPSED_TIME;
        } finally {
            ((MetaDataManager) metaDataManager).close();
        }
        return (endTime - startTime);
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        String NEW_LINE = System.getProperty("line.separator");

        result.append(this.getClass().getName() + " Object {" + NEW_LINE);
        result.append(" TopicName: " + m_TopicMetadata.topic() + NEW_LINE);
        result.append(" PartitionId: " + m_PartitionMetadata.partitionId() + NEW_LINE);
        result.append("}");

        return result.toString();
    }
}