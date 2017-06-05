//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.threads;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.microsoft.kafkaavailability.discovery.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Utility class acting as a heart-beat to KAT.
 */
public class HeartBeat {

    private final ThreadFactory threadFactory;

    private ScheduledExecutorService scheduler;

    private final String serverName;
    private final long heartBeatIntervalInSeconds;
    final static Logger logger = LoggerFactory.getLogger(HeartBeat.class);

    @Inject
    public HeartBeat(ThreadFactory threadFactory, @Assisted long heartBeatIntervalInSeconds) {
        serverName = CommonUtils.getComputerName();
        this.heartBeatIntervalInSeconds = heartBeatIntervalInSeconds;
        this.threadFactory = threadFactory;
    }

    public void start() {

        scheduler = Executors.newSingleThreadScheduledExecutor(new
                ThreadFactoryBuilder().setNameFormat("HeartBeat-Thread")
                .build());
        logger.info(String.format("Starting heartbeat for %s to run every %d seconds with a zero-second delay time", serverName, heartBeatIntervalInSeconds));

        scheduler.scheduleAtFixedRate(threadFactory.createHeartBeatThread(serverName), 0L, heartBeatIntervalInSeconds, TimeUnit.SECONDS);
    }

    public void stop() {
        logger.info(String.format("Stopping heartbeat for %s", serverName));

        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}