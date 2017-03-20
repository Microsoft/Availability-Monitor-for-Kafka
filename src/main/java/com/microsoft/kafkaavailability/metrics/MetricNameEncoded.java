//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.metrics;

import java.util.concurrent.TimeUnit;

public class MetricNameEncoded {
    public String name;
    public String tag;
    public long clock;

    public MetricNameEncoded(String name, String tag) {
        this.name = name;
        this.tag = tag;
        this.clock = System.currentTimeMillis();
    }

    public long timeInSeconds() {
        return TimeUnit.MILLISECONDS.toSeconds(clock);
    }
}
