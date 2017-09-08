//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.threads;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.Gson;
import com.microsoft.kafkaavailability.metrics.AvailabilityGauge;
import com.microsoft.kafkaavailability.metrics.MetricNameEncoded;
import com.microsoft.kafkaavailability.metrics.MetricNameEncodedFactory;
import com.microsoft.kafkaavailability.reporters.ScheduledReporterCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartBeatThread implements Runnable {
    final static Logger logger = LoggerFactory.getLogger(HeartBeatThread.class);

    private final ScheduledReporterCollector reporterCollector;
    private final MetricNameEncodedFactory metricNameFactory;

    public HeartBeatThread(ScheduledReporterCollector reporterCollector, MetricNameEncodedFactory metricNameFactory) {
        this.reporterCollector = reporterCollector;
        this.metricNameFactory = metricNameFactory;
    }

    @Override
    public void run() {
        MetricRegistry metrics;
        try {
            metrics = reporterCollector.getRegistry();

            MetricNameEncoded heartbeatAvailability = metricNameFactory.createWithDefaultDimensions("Heartbeat");
            if (!metrics.getNames().contains(new Gson().toJson(heartbeatAvailability))) {
                metrics.register(new Gson().toJson(heartbeatAvailability), new AvailabilityGauge(1, 1));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}