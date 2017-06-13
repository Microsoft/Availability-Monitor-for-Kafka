//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************
/**
 * Created by Akshat Kaul
 */
package com.microsoft.kafkaavailability.properties;

import java.util.List;

public class AppProperties
{
    public String environmentName;
    public String sqlConnectionString;
    public boolean reportKafkaGTMAvailability;
    public boolean reportKafkaIPAvailability;
    public List<String> kafkaGTMIP;
    public List<String> kafkaIP;
    public boolean useCertificateToConnectToKafkaGTM;
    public boolean useCertificateToConnectToKafkaIP;
    public boolean sendProducerAvailability;
    public boolean sendConsumerAvailability;
    public boolean sendProducerTopicAvailability;
    public boolean sendConsumerTopicAvailability;
    public boolean sendProducerPartitionAvailability;
    public boolean sendConsumerPartitionAvailability;
    public boolean sendProducerLatency;
    public boolean sendGTMAvailabilityLatency;
    public boolean sendKafkaIPAvailabilityLatency;
    public boolean sendConsumerLatency;
    public boolean sendProducerTopicLatency;
    public boolean sendConsumerTopicLatency;
    public boolean sendProducerPartitionLatency;
    public boolean sendConsumerPartitionLatency;
    public long producerThreadSleepTime;
    public long consumerThreadSleepTime;
    public long leaderInfoThreadSleepTime;
    public long availabilityThreadSleepTime;
    public int reportInterval;
    public long consumerPartitionTimeoutInSeconds;
    public long consumerTopicTimeoutInSeconds;
    public long mainThreadsTimeoutInSeconds;
    public long heartBeatIntervalInSeconds;
}
