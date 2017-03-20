//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.threads;

import com.microsoft.kafkaavailability.*;
import com.microsoft.kafkaavailability.discovery.CommonUtils;
import com.microsoft.kafkaavailability.properties.MetaDataManagerProperties;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Phaser;
import java.util.concurrent.*;

public class LeaderInfoThread implements Callable<Long> {

    final static Logger m_logger = LoggerFactory.getLogger(LeaderInfoThread.class);
    Phaser m_phaser;
    CuratorFramework m_curatorFramework;
    long m_threadSleepTime;

    public LeaderInfoThread(Phaser phaser, CuratorFramework curatorFramework, long threadSleepTime) {
        this.m_phaser = phaser;
        this.m_curatorFramework = curatorFramework;
        this.m_threadSleepTime = threadSleepTime;
        //this.m_phaser.register(); //Registers/Add a new unArrived party to this phaser.
        //CommonUtils.dumpPhaserState("After registration of LeaderInfoThread", phaser);
    }

    @Override
    public Long call() throws Exception {
        int sleepDuration = 1000;
        long elapsedTime = 0L;

        do {
            m_logger.info(Thread.currentThread().getName() +
                    " - LeaderInfo party has arrived and is working in "
                    + "Phase-" + m_phaser.getPhase());
            long lStartTime = System.nanoTime();

            try {
                RunLeaderInfo();
            } catch (Exception e) {
                m_logger.error(e.getMessage(), e);
            }
            elapsedTime = CommonUtils.stopWatch(lStartTime);
            m_logger.info("LeaderInfo Elapsed: " + elapsedTime + " milliseconds.");

            while (elapsedTime < m_threadSleepTime && !m_phaser.isTerminated()) {
                try {
                    Thread.currentThread().sleep(sleepDuration);
                    elapsedTime = elapsedTime + sleepDuration;
                } catch (InterruptedException ie) {
                    m_logger.error(ie.getMessage(), ie);
                }
            }
            //phaser.arriveAndAwaitAdvance();
            //m_phaser.arriveAndDeregister();
            //CommonUtils.dumpPhaserState("After arrival of LeaderInfoThread", m_phaser);
        } while (!m_phaser.isTerminated());
        m_logger.info("LeaderInfoThread (run()) has been COMPLETED.");
        return Long.valueOf(elapsedTime);
    }

    private void RunLeaderInfo() throws IOException, MetaDataManagerException {
        IPropertiesManager metaDataPropertiesManager = new PropertiesManager<MetaDataManagerProperties>("metadatamanagerProperties.json", MetaDataManagerProperties.class);
        IMetaDataManager metaDataManager = new MetaDataManager(m_curatorFramework, metaDataPropertiesManager);

        //Print all the topic/partition information.
        m_logger.info("Printing all the topic/partition information.");
        metaDataManager.printEverything();

        ((MetaDataManager) metaDataManager).close();
    }
}