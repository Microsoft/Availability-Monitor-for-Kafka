//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.threads;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.microsoft.kafkaavailability.metrics.AvailabilityGauge;
import com.microsoft.kafkaavailability.metrics.MetricNameEncoded;
import com.microsoft.kafkaavailability.reporters.ScheduledReporterCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartBeatThread implements Runnable {
    final static Logger logger = LoggerFactory.getLogger(HeartBeatThread.class);

    private final ScheduledReporterCollector reporterCollector;
    private final String serverName;

    @Inject
    public HeartBeatThread(ScheduledReporterCollector reporterCollector, @Assisted String serverName) {
        this.reporterCollector = reporterCollector;
        this.serverName = serverName;
    }

    @Override
    public void run() {
        MetricRegistry metrics;
        try {

            reporterCollector.start();
            metrics = reporterCollector.getRegistry();

            MetricNameEncoded heartbeatAvailability = new MetricNameEncoded("Heartbeat", serverName);
            if (!metrics.getNames().contains(new Gson().toJson(heartbeatAvailability))) {
                metrics.register(new Gson().toJson(heartbeatAvailability), new AvailabilityGauge(1, 1));
            }
            reporterCollector.report();
            logger.debug(String.format("Heartbeat/progress sent for %s", serverName));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                reporterCollector.stop();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}