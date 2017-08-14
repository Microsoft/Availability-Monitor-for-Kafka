//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************
/**
 * Created by Akshat Kaul
 */
package com.microsoft.kafkaavailability.properties;

public class ConsumerProperties
{
    public int maxReads;
    public int brokerPort;
    public int fetchSize;
    public int bufferSize;
    public int soTimeout;
}
