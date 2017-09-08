//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.curator.framework.CuratorFramework;

import kafka.admin.AdminOperationException;
import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.utils.ZkUtils;
import kafka.utils.ZKStringSerializer$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Kafka utility functions.
 */
public class KafkaUtils {

    public static final int DEFAULT_NUM_OF_PARTITION = 1;
    public static final int DEFAULT_REPLICATION = 3;
    final static Logger m_logger = LoggerFactory.getLogger(KafkaUtils.class);

    static int sessionTimeOutInMs = 15 * 1000; // 15 secs
    static int connectionTimeOutInMs = 10 * 1000; // 10 secs

    private static ZkClient fromCurator(CuratorFramework curatorFramework) {
        // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
        // createTopic() will only seem to work (it will return without error).  The topic will exist in
        // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the topic.
        ZkClient zkClient = new ZkClient(curatorFramework.getZookeeperClient().getCurrentConnectionString(), sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);
        zkClient.waitUntilConnected();
        return zkClient;
    }

    public static void createTopic(String topic, CuratorFramework curatorFramework) {
        createTopic(topic, DEFAULT_NUM_OF_PARTITION, DEFAULT_REPLICATION, curatorFramework);
    }

    /**
     * Creates a Topic.
     *
     * @param topicName             Topic name.
     * @param partitions        Number of partitions for the topic.
     * @param replicationFactor Replication factor.
     * @param curatorFramework CuratorFramework.
     */
    public static void createTopic(String topicName, int partitions, int replicationFactor, CuratorFramework curatorFramework) {
        if (partitions <= 0)
            throw new AdminOperationException("number of partitions must be larger than 0");

        if (replicationFactor <= 0)
            throw new AdminOperationException("replication factor must be larger than 0");

        if (!topicExists(topicName, curatorFramework)) {
            m_logger.info(String.format("Topic %s not found, creating...", topicName));
            ZkClient zkClient = fromCurator(curatorFramework);
            try {
                AdminUtils.createTopic(zkClient, topicName, partitions, replicationFactor, new Properties());
                m_logger.info("Topic created. name: {}, partitions: {}, replicationFactor: {}", topicName,
                        partitions, replicationFactor);
            } catch (TopicExistsException ignore) {
                m_logger.info("Topic exists. name: {}", topicName);
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
            }
        } else {
            m_logger.info(String.format("Topic %s found!", topicName));
        }
    }

    public static boolean topicExists(String topic, CuratorFramework curatorFramework) {
        ZkClient zkClient = fromCurator(curatorFramework);
        try {
            zkClient = fromCurator(curatorFramework);
            return AdminUtils.topicExists(zkClient, topic);
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }
}