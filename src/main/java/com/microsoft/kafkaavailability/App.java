//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.microsoft.kafkaavailability.discovery.CommonUtils;
import com.microsoft.kafkaavailability.discovery.Constants;
import com.microsoft.kafkaavailability.discovery.CuratorClient;
import com.microsoft.kafkaavailability.discovery.CuratorManager;
import com.microsoft.kafkaavailability.properties.AppProperties;
import com.microsoft.kafkaavailability.properties.MetaDataManagerProperties;
import com.microsoft.kafkaavailability.threads.*;
import org.apache.commons.cli.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.log4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/***
 * Sends a canary message to every topic and partition in Kafka.
 * Reads data from the tail of every topic and partition in Kafka
 * Reports the availability and latency metrics for the above operations.
 * Availability is defined as the percentage of total partitions that respond to each operation.
 */
public class App {
    final static Logger m_logger = LoggerFactory.getLogger(App.class);

    static int m_sleepTime = 30000;
    static AppProperties appProperties;
    static MetaDataManagerProperties metaDataProperties;
    static List<String> listServers;

    private static String registrationPath = Constants.DEFAULT_REGISTRATION_ROOT;
    private static String ip = CommonUtils.getIpAddress();
    private static String computerName = CommonUtils.getComputerName();
    private static String serviceSpec = "";

    public static void main(String[] args) throws IOException, MetaDataManagerException, InterruptedException {
        m_logger.info("Starting KafkaAvailability Tool");
        IPropertiesManager appPropertiesManager = new PropertiesManager<>("appProperties.json", AppProperties.class);
        IPropertiesManager metaDataPropertiesManager = new PropertiesManager<>("metadatamanagerProperties.json", MetaDataManagerProperties.class);
        appProperties = (AppProperties) appPropertiesManager.getProperties();
        metaDataProperties = (MetaDataManagerProperties) metaDataPropertiesManager.getProperties();
        Options options = new Options();
        options.addOption("r", "run", true, "Number of runs. Don't use this argument if you want to run infinitely.");
        options.addOption("s", "sleep", true, "Time (in milliseconds) to sleep between each run. Default is 30000");
        options.addOption("c", "cluster", true, "Cluster name. will pull from here if appProperties is null");
        options.addOption("p", "keyStorePassword", true, "Password to load KeyStore file, which is uesed to connect to cluster's secured public IP");

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        final CuratorFramework curatorFramework = CuratorClient.getCuratorFramework(metaDataProperties.zooKeeperHosts);

        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);
            int howManyRuns;

            if(appProperties.environmentName == null || appProperties.environmentName.equals("")) {
                if(line.hasOption("cluster")) {
                    appProperties.environmentName = line.getOptionValue("cluster");   
                } else {
                    throw new IllegalArgumentException("cluster name must be provided either on the command line or in the app properties");
                }
            }

            if(line.hasOption("keyStorePassword")) {
                appProperties.keyStoreFilePassword = line.getOptionValue("keyStorePassword");
            }
            
            MDC.put("cluster", appProperties.environmentName);

            MDC.put("computerName", computerName);
            CuratorManager curatorManager = CallRegister(curatorFramework);

            if (line.hasOption("sleep")) {
                m_sleepTime = Integer.parseInt(line.getOptionValue("sleep"));
            }

            if (line.hasOption("run")) {
                howManyRuns = Integer.parseInt(line.getOptionValue("run"));
                for (int i = 0; i < howManyRuns; i++) {
                    waitForChanges(curatorManager);
                    RunOnce(curatorFramework);
                    Thread.sleep(m_sleepTime);
                }
            } else {
                while (true) {
                    waitForChanges(curatorManager);
                    RunOnce(curatorFramework);
                    Thread.sleep(m_sleepTime);
                }
            }
        } catch (ParseException exp) {
            // oops, something went wrong
            m_logger.error("Parsing failed.  Reason: " + exp.getMessage());
            formatter.printHelp("KafkaAvailability", options);
        } catch (Exception e) {
            m_logger.error(e.getMessage(), e);
        }
        //used to run shutdown hooks before the program quits. The shutdown hooks (if properly set up) take care of doing all necessary shutdown ceremonies such as closing files, releasing resources etc.
        System.exit(0);
    }


    private static CuratorManager CallRegister(final CuratorFramework curatorFramework) throws Exception {
        int port = ((int) (65535 * Math.random()));
        serviceSpec = ip + ":" + Integer.valueOf(port).toString();

        String basePath = new StringBuilder().append(registrationPath).toString();
        m_logger.info("Creating client, KAT in the Environment:" + appProperties.environmentName);

        final CuratorManager curatorManager = new CuratorManager(curatorFramework, basePath, ip, serviceSpec);

        try {
            curatorManager.registerLocalService();

            Runtime.getRuntime().addShutdownHook(new Thread() {

                @Override
                public void run() {
                    m_logger.info("Normal shutdown executing.");
                    curatorManager.unregisterService();
                    if (curatorFramework != null && (curatorFramework.getState().equals(CuratorFrameworkState.STARTED) || curatorFramework.getState().equals(CuratorFrameworkState.LATENT))) {
                        curatorFramework.close();
                    }
                }
            });
        } catch (Exception e) {
            m_logger.error(e.getMessage(), e);
        }

        return curatorManager;
    }

    private static void waitForChanges(CuratorManager curatorManager) throws Exception {

        try {
            //wait for rest clients to warm up.
            Thread.sleep(5000);
            listServers = curatorManager.listServiceInstance();
            m_logger.info("Environment Name:" + appProperties.environmentName + ". List of KAT Clients:" + Arrays.toString(listServers.toArray()));

            curatorManager.verifyRegistrations();
        } catch (Exception e) {
                /*                 * Something bad did happen, but carry on
                 */
            m_logger.error(e.getMessage(), e);
        }
    }

    private static void RunOnce(CuratorFramework curatorFramework) throws IOException, MetaDataManagerException {

        //default to 1 minute, if not configured
        long heartBeatIntervalInSeconds = (appProperties.heartBeatIntervalInSeconds > 0 ? appProperties.heartBeatIntervalInSeconds : 60);
        HeartBeat beat = new HeartBeat(appProperties.environmentName, heartBeatIntervalInSeconds);
        beat.start();

        /** The phaser is a nice synchronization barrier. */
        final Phaser phaser = new Phaser(1) {
            /**
             * Every time before advancing to next phase overridden
             * onAdvance() method is called and returns either true or false.
             * onAdvance() is invoked when all threads reached the synchronization barrier. It returns true if the
             * phaser should terminate, false if phaser should continue with next phase. When terminated: (1) attempts
             * to register new parties have no effect and (2) synchronization methods immediately return without waiting
             * for advance. When continue:
             * <p>
             * <pre>
             *       -> set unarrived parties = registered parties
             *       -> set arrived parties = 0
             *       -> set phase = phase + 1
             * </pre>
             * <p>
             * This causes another iteration for all thread parties in a new phase (cycle).
             */
            protected boolean onAdvance(int phase, int registeredParties) {
                m_logger.info("onAdvance() method" + " -> Registered: " + getRegisteredParties() + " - Unarrived: "
                        + getUnarrivedParties() + " - Arrived: " + getArrivedParties() + " - Phase: " + getPhase());

            /*return true after completing phase-1 or
            * if  number of registeredParties become 0
            */

                if (phase == 0) {
                    m_logger.info("onAdvance() method, returning true, hence phaser will terminate");
                    return true;
                } else {
                    m_logger.info("onAdvance() method, returning false, hence phaser will continue");
                    return false;
                }
            }
        };

        //default to 1 minute, if not configured
        long producerThreadSleepTime = (appProperties.producerThreadSleepTime > 0 ? appProperties.producerThreadSleepTime : 60000);

        //default to 1 minute, if not configured
        long availabilityThreadSleepTime = (appProperties.availabilityThreadSleepTime > 0 ? appProperties.availabilityThreadSleepTime : 60000);

        //default to 5 minutes, if not configured
        long leaderInfoThreadSleepTime = (appProperties.leaderInfoThreadSleepTime > 0 ? appProperties.leaderInfoThreadSleepTime : 300000);

        //default to 5 minutes, if not configured
        long consumerThreadSleepTime = (appProperties.consumerThreadSleepTime > 0 ? appProperties.consumerThreadSleepTime : 300000);

        //default to 10 minutes, if not configured
        long mainThreadsTimeoutInSeconds = (appProperties.mainThreadsTimeoutInSeconds > 0 ? appProperties.mainThreadsTimeoutInSeconds : 60);

        ExecutorService service = Executors.newFixedThreadPool(4, new
                ThreadFactoryBuilder().setNameFormat("Main-ExecutorService-Thread")
                .build());

        /*
        Adding the thread timeout to make sure, we never end up with long running thread which are never finishing.

        ConsumerThread usually takes longer to finish as it has to initiate multiple child threads for consuming data from each topic and partition.
        Adding one extra minute to other threads so that they can finish the current execution (they perform same operation multiple times) otherwise they may also get get interupted.
         */
        JobManager LeaderInfoJob = new JobManager(mainThreadsTimeoutInSeconds , TimeUnit.SECONDS, new LeaderInfoThread(phaser, curatorFramework, leaderInfoThreadSleepTime), "LeaderInfoThread");
        JobManager ProducerJob = new JobManager(mainThreadsTimeoutInSeconds , TimeUnit.SECONDS, new ProducerThread(phaser, curatorFramework, producerThreadSleepTime, appProperties.environmentName), "ProducerThread");
        JobManager AvailabilityJob = new JobManager(mainThreadsTimeoutInSeconds , TimeUnit.SECONDS, new AvailabilityThread(phaser, curatorFramework, availabilityThreadSleepTime, appProperties.environmentName), "AvailabilityThread");
        JobManager ConsumerJob = new JobManager(mainThreadsTimeoutInSeconds, TimeUnit.SECONDS, new ConsumerThread(phaser, curatorFramework, listServers, serviceSpec, appProperties.environmentName, consumerThreadSleepTime), "ConsumerThread");

        service.submit(LeaderInfoJob);
        service.submit(ProducerJob);
        service.submit(AvailabilityJob);
        service.submit(ConsumerJob);

        CommonUtils.dumpPhaserState("Before main thread arrives and deregisters", phaser);
        //Wait for the consumer thread to finish, Rest other threads can keep running multiple times, while the consumer thread is executing.
        while (!phaser.isTerminated()) {

            /**
             * This block will try a TimeoutException. Why ? Because the phase isn't advanced.
             * It will be advanced when all registered tasks will invoke arrive* method.
             */

            try {
                //Awaits the phase of this phaser to advance from the given phase value or the given timeout to elapse, throwing InterruptedException if interrupted while waiting,
                // or returning immediately if the current phase is not equal to the given phase value or this phaser is terminated.
                phaser.awaitAdvanceInterruptibly(phaser.arrive(), mainThreadsTimeoutInSeconds, TimeUnit.SECONDS);
            } catch (TimeoutException te) {
                m_logger.error("Super thread timed out for " + mainThreadsTimeoutInSeconds + " " + TimeUnit.SECONDS + ", but super thread will advance.");
            } catch (InterruptedException ie) {
            }

            try {
                /*
                * arriveAndDeregister deregisters reduces the number of arrived parties
                * arriveAndDeregister() throws IllegalStateException if number of
                * registered or unarrived parties would become negative
                * Calling arriveAndDeregister, just in case the thread had an exception(timeout configured in job manager call) but phaser is still not arrived.
                * */
                phaser.arriveAndDeregister();

                /*
                * arrive() returns a negative number if the Phaser is terminated
                 */
                phaser.forceTermination();
            } catch (IllegalStateException exception) {
            }
        }

        //shut down the executor service now. This will make the executor accept no new threads
        // and finish all existing threads in the queue
        CommonUtils.shutdownAndAwaitTermination(service, "Main-ExecutorService-Thread");

        /**
         * When the final party for a given phase arrives, onAdvance() is invoked and the phase advances. The
         * "face advances" means that all threads reached the barrier and therefore all threads are synchronized and can
         * continue processing.
         */

        /**
         * The arrival and deregistration of the main thread allows the other threads to start working. This is because
         * now the registered parties equal the arrived parties.
         */
        // deregistering the main thread
        phaser.arriveAndDeregister();
        //CommonUtils.dumpPhaserState("After main thread arrived and deregistered", phaser);

        beat.stop();
        m_logger.info("All Finished.");
    }
}