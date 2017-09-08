//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability;

import kafka.javaapi.TopicMetadata;

import java.util.Comparator;

public class TopicMetadataComparator implements Comparator<TopicMetadata> {
    @Override
    public int compare(TopicMetadata t1, TopicMetadata t2) {
        return (t1.topic()).compareToIgnoreCase((t2.topic()));
    }
}