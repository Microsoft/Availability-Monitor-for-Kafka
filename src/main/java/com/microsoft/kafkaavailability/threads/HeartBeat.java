//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.threads;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.microsoft.kafkaavailability.properties.AppProperties;
import com.microsoft.kafkaavailability.reporters.ScheduledReporterCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Utility class acting as a heart-beat to KAT.
 */
public class HeartBeat {

    private final ScheduledReporterCollector scheduledReporterCollector;
    private final ScheduledExecutorService scheduler;
    private final String serverName;
    private final long heartBeatIntervalInSeconds;
    final static Logger LOGGER = LoggerFactory.getLogger(HeartBeat.class);

    @Inject
    public HeartBeat(ScheduledReporterCollector scheduledReporterCollector, AppProperties appProperties,
                     @Named("localHostName") String localHostName,
                     @Named("hearBeatExecutorService") ScheduledExecutorService scheduledExecutorService) {

        this.scheduledReporterCollector = scheduledReporterCollector;
        //default to 1 minute, if not configured
        this.heartBeatIntervalInSeconds = (appProperties.heartBeatIntervalInSeconds > 0 ? appProperties.heartBeatIntervalInSeconds : 30);

        this.serverName = localHostName;
        this.scheduler = scheduledExecutorService;
    }

    public void start() {
        LOGGER.info(String.format("Starting heartbeat for %s to run every %d seconds with a zero-second delay time", serverName, heartBeatIntervalInSeconds));

        scheduledReporterCollector.start();
        scheduler.scheduleAtFixedRate(new HeartBeatThread(scheduledReporterCollector, serverName), 0L, heartBeatIntervalInSeconds, TimeUnit.SECONDS);
    }

    public void stop() {
        LOGGER.info(String.format("Stopping heartbeat for %s", serverName));

        if (scheduler != null) {
            scheduler.shutdownNow();
        }

        scheduledReporterCollector.stop();
    }
}