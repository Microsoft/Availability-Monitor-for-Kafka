//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.discovery;

import com.google.common.base.Throwables; 
 
import org.apache.curator.framework.CuratorFramework; 
import org.apache.curator.framework.CuratorFrameworkFactory; 
import org.apache.curator.framework.imps.CuratorFrameworkState; 
import org.apache.curator.retry.ExponentialBackoffRetry; 
import org.apache.curator.utils.EnsurePath; 
 
import org.slf4j.Logger; 
import org.slf4j.LoggerFactory; 
 
/** * The Class CuratorClient. 
 */ 
public class CuratorClient {

    private static Logger log = LoggerFactory.getLogger(CuratorClient.class);

    public static final int DEFAULT_MAX_SLEEP_MS = 60000;

    /**
     * Gets the curator framework.
     *
     * @param zkConnectionString the zoo keeper connection string
     * @return the curator framework
     */
    public static CuratorFramework getCuratorFramework(String zkConnectionString) {
        return getCuratorFramework(getCuratorBuilder(zkConnectionString));
    }

    /**
     * Get a framework using a builder argument.
     *
     * @param builder The builder to use.
     * @return the curator framework
     */
    public static CuratorFramework getCuratorFramework(CuratorFrameworkFactory.Builder builder) {
        CuratorFramework curatorFramework = builder.build();
        init(curatorFramework);

        try {
            curatorFramework.getZookeeperClient().blockUntilConnectedOrTimedOut();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            Throwables.propagate(e);
        }

        return curatorFramework;
    }

    /**
     * Get a builder object and allow user to override specific parameters.
     *
     * @param zkConnectionString Zookeeper connection string.
     * @return A builder.
     */
    public static CuratorFrameworkFactory.Builder getCuratorBuilder(String zkConnectionString) {
        return CuratorFrameworkFactory.builder()
                .connectionTimeoutMs(10 * 1000)
                .retryPolicy(new ExponentialBackoffRetry(10, 20, DEFAULT_MAX_SLEEP_MS))
                .connectString(zkConnectionString);
    }

    public static void init(CuratorFramework curatorFramework) {
        if (!curatorFramework.getState().equals(CuratorFrameworkState.STARTED)) {
            curatorFramework.start();
            log.info("Curator Framework Client Started. ");
        }
    }

    public static void registerForChanges(CuratorFramework curatorFramework,
                                          String... basePaths) {

        if (curatorFramework.getState() != CuratorFrameworkState.STARTED) {
            curatorFramework.start();
        }

        try {
            curatorFramework.getZookeeperClient().blockUntilConnectedOrTimedOut();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            Throwables.propagate(e);
        }

        for (String basePath : basePaths) {

            log.debug("Adding watched path: " + basePath);

            try {
                new EnsurePath(basePath).ensure(curatorFramework.getZookeeperClient());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                Throwables.propagate(e);
            }
        }

        log.debug("exiting registerForChanges");
    }
}
