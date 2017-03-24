//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.threads;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.Gson;
import com.microsoft.kafkaavailability.*;
import com.microsoft.kafkaavailability.metrics.AvailabilityGauge;
import com.microsoft.kafkaavailability.metrics.MetricNameEncoded;
import com.microsoft.kafkaavailability.metrics.MetricsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class HeartBeatThread implements Runnable {
    final static Logger logger = LoggerFactory.getLogger(HeartBeatThread.class);
    private MetricsFactory metricsFactory;
    private final String serverName;
    private final String clusterName;

    public HeartBeatThread(String clusterName, String serverName) {
        this.serverName = serverName;
        this.clusterName = clusterName;
    }

    @Override
    public void run() {
        MetricRegistry metrics;
        try {
            metricsFactory = new MetricsFactory();
            metricsFactory.configure(clusterName);

            metricsFactory.start();
            metrics = metricsFactory.getRegistry();

            MetricNameEncoded heartbeatAvailability = new MetricNameEncoded("Heartbeat", serverName);
            if (!metrics.getNames().contains(new Gson().toJson(heartbeatAvailability))) {
                metrics.register(new Gson().toJson(heartbeatAvailability), new AvailabilityGauge(1, 1));
            }
            metricsFactory.report();
            logger.debug(String.format("Heartbeat/progress sent for %s", serverName));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                metricsFactory.stop();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}