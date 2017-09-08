//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************


package com.microsoft.kafkaavailability;

import com.google.gson.Gson;
import com.microsoft.kafkaavailability.properties.MetaDataManagerProperties;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import static com.microsoft.kafkaavailability.discovery.CommonUtils.createTopicRegEx;

/***
 * Gets the list of Kafka brokers.
 * Gets Topic and Partition metadata.
 */
public class MetaDataManager implements IMetaDataManager
{
    final static Logger m_logger = LoggerFactory.getLogger(MetaDataManager.class);
    private CuratorFramework client;

    MetaDataManagerProperties m_mDProps;
    List<String> m_brokerIds;

    public MetaDataManager(CuratorFramework curatorFramework, IPropertiesManager<MetaDataManagerProperties> propManager)
    {
        m_mDProps = propManager.getProperties();
        this.client = curatorFramework;
    }

    /***
     * Gets list of broker IPs from the properties file in the form 1.1.1.1:2181,2.2.2.2:2181
     *
     * @param addPort If true, adds port to the broker ip
     * @return List of broker IPs
     */
    @Override
    public List<String> getBrokerList(boolean addPort) throws MetaDataManagerException
    {
        if (m_mDProps.useZooKeeper)
        {
            m_brokerIds = getBrokerListFromZooKeeper(addPort);
        } else
        {
            m_brokerIds = m_mDProps.brokerList;
        }
        return m_brokerIds;
    }

    @Override
    public void createTopicIfNotExist(String topicName, int partitions, int replicationFactor) {
        KafkaUtils.createTopic(topicName, partitions, replicationFactor, client);
    }

    @Override
    public void createCanaryTopics() {
        List<String> topics = new ArrayList<String>();
        if (!m_mDProps.canaryTestTopics.isEmpty()) {
            topics.addAll(m_mDProps.canaryTestTopics);
            int replicationFactor = (m_mDProps.replicationFactor > 0) ? m_mDProps.replicationFactor : 3;
            try {
                List<String> brokerList = getBrokerList(true);

                for (String topicName : topics) {
                    createTopicIfNotExist(topicName, brokerList.size(), replicationFactor);
                }
            } catch (Exception e) {
                m_logger.error(e.toString());
            }
        }
    }

    /***
     * Gets list of broker IPs from zookeeper in the form 1.1.1.1:2181,2.2.2.2:2181
     *
     * @param addPort If true, adds port to the broker ip
     * @return List of broker IPs
     */
    private List<String> getBrokerListFromZooKeeper(boolean addPort) throws MetaDataManagerException
    {
        List<String> brokerInfos = new ArrayList<String>();
        Gson gson = new Gson();
        int exceptionCount = 0;
        try
        {
            List<String> ids = client.getChildren().forPath("/brokers/ids");

            for (String id : ids)
            {
                String brokerInfoStr = new String(client.getData().forPath("/brokers/ids/" + id));
                BrokerInfo brokerInfo = gson.fromJson(brokerInfoStr, BrokerInfo.class);
                if (addPort)
                {
                    brokerInfos.add(brokerInfo.host + ":" + brokerInfo.port);
                } else
                {
                    brokerInfos.add(brokerInfo.host);
                }
            }
        } catch (ArrayIndexOutOfBoundsException e)
        {
            exceptionCount++;
            if (exceptionCount < m_mDProps.acceptable_exception_count)
            {
                m_logger.error(e.toString());
            } else
            {
                throw new MetaDataManagerException(e.getMessage());
            }
        } catch (IOException e)
        {
            throw new MetaDataManagerException(e.getMessage());
        } catch(InterruptedException e)
        {
            throw new MetaDataManagerException(e.getMessage());
        }catch (KeeperException e)
        {
            throw new MetaDataManagerException(e.getMessage());
        }
        catch (Exception e)
        {
            throw new MetaDataManagerException(e.getMessage());
        }

        if (brokerInfos.isEmpty())
        {
            m_logger.info("Unable to get BrokerList From ZooKeeper. Check brokers if available.");
        }
        return brokerInfos;
    }

    /***
     * Gets metadata for all topics in the cluster
     *
     * @return List topic metadata
     */
    @Override
    public List<kafka.javaapi.TopicMetadata> getMetaDataFromAllBrokers() {
        List<String> topics = new ArrayList<String>();
        try {
            m_brokerIds = getBrokerList(true);
        } catch (Exception e) {
            m_logger.error(e.toString());
        }
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        List<kafka.javaapi.TopicMetadata> allMetaData = new ArrayList<kafka.javaapi.TopicMetadata>();
        for (String brokerId : m_brokerIds) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(
                        brokerId.split(":")[0],
                        Integer.parseInt(brokerId.split(":")[1]),
                        m_mDProps.soTimeout,
                        m_mDProps.bufferSize,
                        m_mDProps.clientId);
                kafka.javaapi.TopicMetadataResponse resp = null;
                resp = consumer.send(req);
                allMetaData.addAll(resp.topicsMetadata());
            } catch (Exception e) {
                m_logger.error("Error communicating with Broker [" + brokerId + "]. Reason: " + e.getMessage(), e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        return allMetaData;
    }

    /***
     * Dedupe the partition metadata from all brokers
     *
     * @return Deduped topic metadata
     */
    public List<kafka.javaapi.TopicMetadata> getAllTopicPartition()
    {
        List<kafka.javaapi.TopicMetadata> topicMetadataList = getMetaDataFromAllBrokers();
        HashSet<TopicPartition> exploredTopicPartition = new HashSet<TopicPartition>();
        List<kafka.javaapi.TopicMetadata> ret = new ArrayList<TopicMetadata>();

        // Filter any white list topics
        HashSet<String> whiteListTopics = new HashSet<String>(m_mDProps.whiteListTopics);
        if (!whiteListTopics.isEmpty()) {
            topicMetadataList = filterWhitelistTopics(topicMetadataList, whiteListTopics);
        }

        // Filter all blacklist topics
        HashSet<String> blackListTopics = new HashSet<String>(m_mDProps.blackListTopics);
        String regex = "";
        if (!blackListTopics.isEmpty()) {
            regex = createTopicRegEx(blackListTopics);
        }

        for (TopicMetadata item : topicMetadataList)
        {
            if (Pattern.matches(regex, item.topic())) {
                m_logger.debug("Discarding topic (blacklisted): " + item.topic());
                continue;
            }
            List<kafka.api.PartitionMetadata> pml = new ArrayList<kafka.api.PartitionMetadata>();
            for (PartitionMetadata part : item.partitionsMetadata())
            {
                if (!exploredTopicPartition.contains(new TopicPartition(item.topic(), part.partitionId())))
                {
                    kafka.api.PartitionMetadata pm =
                            new kafka.api.PartitionMetadata(
                                    part.partitionId(),
                                            Option.apply(part.leader()),
                                            JavaConversions.asScalaBuffer(part.replicas()).toList(),
                                            JavaConversions.asScalaBuffer(part.isr()).toList(),
                                            part.errorCode());
                    pml.add(pm);
                    exploredTopicPartition.add(new TopicPartition(item.topic(), part.partitionId()));
                }
            }
            if (pml.size() > 0)
            {
                kafka.api.TopicMetadata tm =
                        new kafka.api.TopicMetadata(
                                item.topic(),
                                JavaConversions.asScalaBuffer(pml).toList(),
                                item.errorCode());
                ret.add(new kafka.javaapi.TopicMetadata(tm));
            }
        }
        Collections.sort(ret, new TopicMetadataComparator());
        return ret;
    }

    public List<TopicMetadata> filterWhitelistTopics(List<TopicMetadata> topicMetadataList,
                                                     HashSet<String> whiteListTopics) {
        ArrayList<TopicMetadata> filteredTopics = new ArrayList<TopicMetadata>();
        String regex = createTopicRegEx(whiteListTopics);
        for (TopicMetadata topicMetadata : topicMetadataList) {
            if (Pattern.matches(regex, topicMetadata.topic())) {
                filteredTopics.add(topicMetadata);
            } else {
                m_logger.debug("Discarding topic (non-whitelisted): " + topicMetadata.topic());
            }
        }
        return filteredTopics;
    }

    /***
     * Print all the metadata
     */
    @Override
    public void printEverything()
    {
        List<kafka.javaapi.TopicMetadata> data = getAllTopicPartition();
        for (kafka.javaapi.TopicMetadata item : data)
        {
            m_logger.info("Topic: " + item.topic());
            for (kafka.javaapi.PartitionMetadata part : item.partitionsMetadata())
            {
                String replicas = "";
                String isr = "";
                for (kafka.cluster.Broker replica : part.replicas())
                {
                    replicas += " " + replica.host();
                }
                for (kafka.cluster.Broker replica : part.isr())
                {
                    isr += " " + replica.host();
                }
                String leader = "";
                if (part.leader() != null)
                {
                    if (part.leader().host() != null)
                        leader = part.leader().host();
                }
                m_logger.info("    Partition: " + part.partitionId() + ": Leader: " + leader + " Replicas:[" + replicas + "] ISR:[" + isr + "]");
            }
        }
    }

    /***
     * Releases all resources
     */
    public void close()
    {
        if(client != null)
        {
            m_logger.debug("Closing the client");
        }
    }
}
