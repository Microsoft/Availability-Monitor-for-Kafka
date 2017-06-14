//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability;

import java.io.IOException;

public interface IProducer
{
    void sendCanaryToTopicPartition(String topicName, String partitionId);
    void sendCanaryToKafkaIP(String kafkaIP, String topicName, boolean enableCertCheck, String keyStorePath, String keyStorePassword) throws Exception;
    void close() throws IOException;
}
