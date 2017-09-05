//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.multibindings.MultibindingsScanner;
import com.microsoft.kafkaavailability.discovery.CommonUtils;
import com.microsoft.kafkaavailability.discovery.CuratorManager;
import com.microsoft.kafkaavailability.module.AppModule;
import com.microsoft.kafkaavailability.module.ModuleScanner;
import com.microsoft.kafkaavailability.module.ReportersModule;
import com.microsoft.kafkaavailability.module.MonitorTasksModule;
import com.microsoft.kafkaavailability.properties.AppProperties;
import com.microsoft.kafkaavailability.properties.MetaDataManagerProperties;
import com.microsoft.kafkaavailability.reporters.ScheduledReporterCollector;
import com.microsoft.kafkaavailability.threads.HeartBeat;
import com.microsoft.kafkaavailability.threads.JobManager;
import com.microsoft.kafkaavailability.threads.MonitorTaskFactory;
import org.apache.commons.cli.*;
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

    private static String computerName = CommonUtils.getComputerName();

    public static void main(String[] args) throws IOException, MetaDataManagerException, InterruptedException {
        m_logger.info("Starting KafkaAvailability Tool");

        Options options = new Options();
        options.addOption("r", "run", true, "Number of runs. Don't use this argument if you want to run infinitely.");
        options.addOption("s", "sleep", true, "Time (in milliseconds) to sleep between each run. Default is 30000");
        options.addOption("c", "cluster", true, "Cluster name. will pull from here if appProperties is null");
        options.addOption("p", "keyStorePassword", true, "Password to load KeyStore file, which is uesed to connect to cluster's secured public IP");

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        CommandLine line;
        try {
            // parse the command line arguments
            line = parser.parse(options, args);
        } catch (ParseException exp) {
            // oops, something went wrong
            m_logger.error("Parsing failed.  Reason: " + exp.getMessage());
            formatter.printHelp("KafkaAvailability", options);
            throw new RuntimeException("Failed to parse app variables");
        }

        Injector parent = Guice.createInjector(new AppModule(line, "reporterProperties.json", "appProperties.json", "metadatamanagerProperties.json"));

        ImmutableSet<Module> allGuiceModules = ImmutableSet.<Module>builder()
                .add(MultibindingsScanner.asModule())
                .add(parent.getInstance(ReportersModule.class))
                .add(parent.getInstance(MonitorTasksModule.class))
                .addAll(ModuleScanner.getModulesFromDependencies())
                .build();

        //Parent injector to process command line arguments and json files

        Injector injector = parent.createChildInjector(allGuiceModules.toArray(new Module[allGuiceModules.size()]));

        appProperties = injector.getInstance(AppProperties.class);
        metaDataProperties = injector.getInstance(MetaDataManagerProperties.class);

        final CuratorManager curatorManager = injector.getInstance(CuratorManager.class);
        final MonitorTaskFactory monitorTaskFactory = injector.getInstance(MonitorTaskFactory.class);
        final ScheduledReporterCollector reporterCollector = injector.getInstance(ScheduledReporterCollector.class);
        reporterCollector.start();
        final HeartBeat heartBeat = injector.getInstance(HeartBeat.class);

        MDC.put("cluster", appProperties.environmentName);
        MDC.put("computerName", computerName);

        if (line.hasOption("sleep")) {
            m_sleepTime = Integer.parseInt(line.getOptionValue("sleep"));
        }

        heartBeat.start();
        int howManyRuns;

        if (line.hasOption("run")) {
            howManyRuns = Integer.parseInt(line.getOptionValue("run"));
            for (int i = 0; i < howManyRuns; i++) {
                waitForChanges(curatorManager);
                runOnce(monitorTaskFactory, reporterCollector);
                Thread.sleep(m_sleepTime);
            }
        } else {
            while (true) {
                waitForChanges(curatorManager);
                runOnce(monitorTaskFactory, reporterCollector);
                Thread.sleep(m_sleepTime);
            }
        }

        //used to run shutdown hooks before the program quits. The shutdown hooks (if properly set up) take care of doing all necessary shutdown ceremonies such as closing files, releasing resources etc.
        System.exit(0);
        if (heartBeat != null) {
            heartBeat.stop();
        }

        reporterCollector.report();
        reporterCollector.stop();
    }

    private static void waitForChanges(CuratorManager curatorManager) {

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

    private static void runOnce(MonitorTaskFactory monitorTaskFactory, ScheduledReporterCollector reporterCollector) throws IOException, MetaDataManagerException {

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
        JobManager LeaderInfoJob = new JobManager(mainThreadsTimeoutInSeconds , TimeUnit.SECONDS, monitorTaskFactory.createLeaderInfoThread(phaser, leaderInfoThreadSleepTime), "LeaderInfoThread");
        JobManager ProducerJob = new JobManager(mainThreadsTimeoutInSeconds , TimeUnit.SECONDS, monitorTaskFactory.createProducerThread(phaser, producerThreadSleepTime), "ProducerThread");
        JobManager AvailabilityJob = new JobManager(mainThreadsTimeoutInSeconds , TimeUnit.SECONDS, monitorTaskFactory.createAvailabilityThread(phaser, availabilityThreadSleepTime), "AvailabilityThread");
        JobManager ConsumerJob = new JobManager(mainThreadsTimeoutInSeconds, TimeUnit.SECONDS, monitorTaskFactory.createConsumerThread(phaser, listServers, consumerThreadSleepTime), "ConsumerThread");

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

        reporterCollector.stop();
        m_logger.info("All Finished.");
    }
}