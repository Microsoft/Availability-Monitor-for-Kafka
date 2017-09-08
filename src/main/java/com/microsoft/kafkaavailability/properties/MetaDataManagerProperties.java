//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************
/**
 * Created by Akshat Kaul
 */

package com.microsoft.kafkaavailability.properties;

import java.util.List;
public class MetaDataManagerProperties
{
    public List<String> brokerList;
    public List<String> whiteListTopics;
    public List<String> canaryTestTopics;
    public int replicationFactor;
    public int soTimeout;
    public int bufferSize;
    public String clientId;
    public String zooKeeperHosts;
    public boolean useZooKeeper;
    public Integer acceptable_exception_count;
    public List<String> blackListTopics;
}
