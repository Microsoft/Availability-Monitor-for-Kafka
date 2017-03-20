//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability;

import java.util.List;

public interface IMetaDataManager
{
    List<String> getBrokerList(boolean addPort) throws MetaDataManagerException;
    List<kafka.javaapi.TopicMetadata> getMetaDataFromAllBrokers();
    List<kafka.javaapi.TopicMetadata> getAllTopicPartition();
    void createTopicIfNotExist(String topicName, int partitions, int replicationFactor);
    void createCanaryTopics();
    void printEverything();
}
