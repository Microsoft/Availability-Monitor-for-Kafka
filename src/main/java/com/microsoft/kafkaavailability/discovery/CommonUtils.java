//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.discovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class CommonUtils {

    private static Logger log = LoggerFactory.getLogger(CommonUtils.class);

    /**
     * Get the ip address of this host
     */
    public static String getIpAddress() {
        String ipAddress = null;

        try {

            InetAddress iAddress = InetAddress.getLocalHost();
            ipAddress = iAddress.getHostAddress();
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage(), e);
        }
        return (ipAddress);
    }

    /**
     * Get the ComputerName of this host
     */
    public static String getComputerName() {
        String hostname = "Unknown";

        try {
            InetAddress iAddress = InetAddress.getLocalHost();
            hostname = iAddress.getHostName();
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage(), e);
            hostname = getIpAddress();
        }
        return (hostname);
    }

    /**
     * Method to measure elapsed time since start time until now
     * <p>
     * Example:
     * long startup = System.nanoTime();
     * Thread.sleep(3000);
     * System.out.println("This took " + stopWatch(startup) + " milliseconds.");
     *
     * @param startTime Time of start in Nanoseconds
     * @return Elapsed time in seconds as double
     */
    public static long stopWatch(long startTime) {
        long elapsedTime = System.nanoTime() - startTime;
        return TimeUnit.MILLISECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
    }

    public static void dumpPhaserState(String when, Phaser phaser) {
        log.info(when + " -> Registered: " + phaser.getRegisteredParties() + " - Unarrived: "
                + phaser.getUnarrivedParties() + " - Arrived: " + phaser.getArrivedParties() + " - Phase: "
                + phaser.getPhase());
    }


    /*
     * Returns the next wait interval, in milliseconds, using an exponential
     * backoff algorithm.
     */
    public static long getWaitTimeExp(int retryCount, long waitInterval) {

        long waitTime = ((long) Math.pow(2, retryCount) * waitInterval);

        return waitTime;
    }

    /**
     * Sleep for the specified time, ignoring any exceptions that occur
     *
     * @param millis The number of milliseconds to sleep for
     */
    public static void sleep(long millis) {
        try {
            Thread.currentThread().sleep(millis);
        } catch (InterruptedException e) {
            // Do nothing
        }
    }

    public static String createTopicRegEx(HashSet<String> topicsSet) {
        String regex = "";
        StringBuilder stringbuilder = new StringBuilder();
        for (String whiteList : topicsSet) {
            stringbuilder.append(whiteList);
            stringbuilder.append("|");
        }
        regex = "(" + stringbuilder.substring(0, stringbuilder.length() - 1) + ")";
        Pattern.compile(regex);
        return regex;
    }

    /**
     * <p>Checks if a String is empty ("") or null.</p>
     * <p>
     * <pre>
     * CommonUtils.isNullorEmptyorWhitespace(null)      = true
     * CommonUtils.isNullorEmptyorWhitespace("")        = true
     * CommonUtils.isNullorEmptyorWhitespace("test")     = false
     * CommonUtils.isNullorEmptyorWhitespace("  test  ") = false
     * </pre>
     *
     * @param obj the String to check, may be null
     * @return <code>true</code> if the String is null, empty or whitespace
     */
    public static Boolean isNullorEmptyorWhitespace(String obj) {
        return obj == null || obj.isEmpty() || obj.trim().isEmpty();
    }


    /**
     * Stop new job from being submitted and wait termination.
     * Taken from official documentation:
     * http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ExecutorService.html
     * @param pool the thread pool
     */
    public static void shutdownAndAwaitTermination(ExecutorService pool, String name) {
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(30, TimeUnit.SECONDS))
                    log.error("Pool did not terminate for the thread: {};", name);
            }
        } catch (InterruptedException ie) {
            log.error("Error occured from {}; Exception: {}", name, ie);
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
}