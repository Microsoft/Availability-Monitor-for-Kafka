//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.threads;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingWindowReservoir;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.microsoft.kafkaavailability.*;
import com.microsoft.kafkaavailability.discovery.CommonUtils;
import com.microsoft.kafkaavailability.metrics.AvailabilityGauge;
import com.microsoft.kafkaavailability.metrics.MetricNameEncoded;
import com.microsoft.kafkaavailability.metrics.MetricNameEncodedFactory;
import com.microsoft.kafkaavailability.properties.AppProperties;
import com.microsoft.kafkaavailability.properties.MetaDataManagerProperties;
import com.microsoft.kafkaavailability.reporters.ScheduledReporterCollector;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.microsoft.kafkaavailability.discovery.Constants.DEFAULT_ELAPSED_TIME;

public class ConsumerThread implements Callable<Long> {

    final static Logger m_logger = LoggerFactory.getLogger(ConsumerThread.class);

    private final ScheduledReporterCollector reporterCollector;
    private final CuratorFramework m_curatorFramework;
    private final ServiceSpecProvider serviceSpecProvider;
    private final MetricNameEncodedFactory metricNameFactory;

    private Phaser m_phaser;
    private List<String> m_listServers;
    private long m_threadSleepTime;

    @Inject
    public ConsumerThread(CuratorFramework curatorFramework, ScheduledReporterCollector reporterCollector,
                          ServiceSpecProvider serviceSpecProvider, MetricNameEncodedFactory metricNameFactory,
                          @Assisted  Phaser phaser, @Assisted List<String> listServers, @Assisted long threadSleepTime) {
        this.m_curatorFramework = curatorFramework;
        this.reporterCollector = reporterCollector;
        this.reporterCollector.start();
        this.serviceSpecProvider = serviceSpecProvider;
        this.metricNameFactory = metricNameFactory;

        this.m_phaser = phaser;
        this.m_phaser.register(); //Registers/Add a new unArrived party to this phaser.
        CommonUtils.dumpPhaserState("After registration of ConsumerThread", phaser);
        m_listServers = listServers;
        this.m_threadSleepTime = threadSleepTime;
    }

    @Override
    public Long call() throws Exception {
        int sleepDuration = 1000;
        long elapsedTime = 0L;
        long lStartTime = System.currentTimeMillis();
        MetricRegistry metrics;
        m_logger.info(Thread.currentThread().getName() +
                " - Consumer party has arrived and is working in "
                + "Phase-" + m_phaser.getPhase());

        try {
            metrics = reporterCollector.getRegistry();
            runConsumer(metrics);
        } catch (Exception e) {
            m_logger.error(e.getMessage(), e);
            try {
                m_phaser.arriveAndDeregister();
            } catch (IllegalStateException success) {
            }
        } finally {
            try {
                reporterCollector.report();
                CommonUtils.sleep(1000);
            } catch (Exception e) {
                m_logger.error(e.getMessage(), e);
            }
        }
        elapsedTime = CommonUtils.stopWatch(lStartTime);
        m_logger.info("Consumer Elapsed: " + elapsedTime + " milliseconds.");

        try {
            m_phaser.arriveAndDeregister();
        } catch (IllegalStateException exception) {
        }

        reporterCollector.stop();
        CommonUtils.dumpPhaserState("After arrival of ConsumerThread", m_phaser);
        m_logger.info("ConsumerThread (run()) has been COMPLETED.");
        return Long.valueOf(elapsedTime);
    }

    private void runConsumer(MetricRegistry metrics) throws IOException, MetaDataManagerException {

        m_logger.info("Starting ConsumerLatency");

        IPropertiesManager metaDataPropertiesManager = new PropertiesManager<MetaDataManagerProperties>("metadatamanagerProperties.json", MetaDataManagerProperties.class);
        IMetaDataManager metaDataManager = new MetaDataManager(m_curatorFramework, metaDataPropertiesManager);

        IPropertiesManager appPropertiesManager = new PropertiesManager<AppProperties>("appProperties.json", AppProperties.class);
        AppProperties appProperties = (AppProperties) appPropertiesManager.getProperties();

        int numPartitionsConsumers = 0;

        // check the number of available processors
        int nThreads = Runtime.getRuntime().availableProcessors();

        //default to 15 Seconds, if not configured
        long consumerPartitionTimeoutInSeconds = (appProperties.consumerPartitionTimeoutInSeconds > 0 ? appProperties.consumerPartitionTimeoutInSeconds : 30);
        long consumerTopicTimeoutInSeconds = (appProperties.consumerTopicTimeoutInSeconds > 0 ? appProperties.consumerTopicTimeoutInSeconds : 60);

        //This is full list of topics
        List<kafka.javaapi.TopicMetadata> totalTopicMetadata = metaDataManager.getAllTopicPartition();
        List<kafka.javaapi.TopicMetadata> allTopicMetadata = new ArrayList<kafka.javaapi.TopicMetadata>();

        String sep = ", ";
        StringBuilder rString = new StringBuilder();

        for (kafka.javaapi.TopicMetadata topic : totalTopicMetadata) {
            //Log the server/topic mapping to know which topic is getting  by which instance of KAT-List<String>
            int topicIndex = totalTopicMetadata.indexOf(topic);
            int serverIndex = (topicIndex % m_listServers.size());
            String client = m_listServers.get(serverIndex);

            String serviceSpec = serviceSpecProvider.getServiceSpec();

            if (serverIndex == m_listServers.indexOf(serviceSpec)) {
                allTopicMetadata.add(topic);
            }
            rString.append(sep).append(topic.topic() + "-->" + client);
        }
        m_logger.info("Mapping of topics and servers:" + rString);

        m_logger.info("totalTopicMetadata size:" + totalTopicMetadata.size());
        m_logger.info("allTopicMetadata size in Consumer:" + allTopicMetadata.size());

        int consumerTryCount = 0;
        int consumerFailCount = 0;

        for (kafka.javaapi.TopicMetadata topic : allTopicMetadata) {
            numPartitionsConsumers += topic.partitionsMetadata().size();
        }

        final SlidingWindowReservoir consumerLatencyWindow = new SlidingWindowReservoir(numPartitionsConsumers);
        Histogram histogramConsumerLatency = new Histogram(consumerLatencyWindow);
        MetricNameEncoded consumerLatency = metricNameFactory.createWithTopic("Consumer.Latency", "all");
        if (!metrics.getNames().contains(new Gson().toJson(consumerLatency))) {
            if (appProperties.sendConsumerLatency) {
                metrics.register(new Gson().toJson(consumerLatency), histogramConsumerLatency);
            }
        }
        for (kafka.javaapi.TopicMetadata item : allTopicMetadata) {
            boolean isTopicAvailable = true;
            m_logger.info("Reading from Topic: {};", item.topic());

            consumerTryCount++;
            final SlidingWindowReservoir topicLatency = new SlidingWindowReservoir(item.partitionsMetadata().size());
            Histogram histogramConsumerTopicLatency = new Histogram(topicLatency);
            MetricNameEncoded consumerTopicLatency = metricNameFactory.createWithTopic("Consumer.Latency", item.topic());
            if (!metrics.getNames().contains(new Gson().toJson(consumerTopicLatency))) {
                if (appProperties.sendConsumerTopicLatency)
                    metrics.register(new Gson().toJson(consumerTopicLatency), histogramConsumerTopicLatency);
            }

            //Get ExecutorService from Executors utility class, thread pool size is number of available processors
            ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(nThreads);

            //create a list to hold the Future object associated with Callable
            //List<Future<Long>> futures = new ArrayList<Future<Long>>();
            Map<Integer, Future<Long>> response = new HashMap<Integer, Future<Long>>();

            for (kafka.javaapi.PartitionMetadata part : item.partitionsMetadata()) {
                m_logger.debug("Reading from Topic: {}; Partition: {};", item.topic(), part.partitionId());

                //Create ConsumerPartitionThread instance
                ConsumerPartitionThread consumerPartitionJob = new ConsumerPartitionThread(m_curatorFramework, item, part);

                //submit Callable tasks to be executed by thread pool
                Future<Long> future = newFixedThreadPool.submit(new JobManager(consumerPartitionTimeoutInSeconds, TimeUnit.SECONDS, consumerPartitionJob, "Consumer-" + item.topic() + "-P#" + part.partitionId()));

                //add Future to the list, we can get return value using Future
                //futures.add(future);
                response.put(part.partitionId(), future);
            }

            //shut down the executor service now. This will make the executor accept no new threads
            // and finish all existing threads in the queue
            CommonUtils.shutdownAndAwaitTermination(newFixedThreadPool, item.topic());

            int topicConsumerFailCount = 0;
            for (Integer key : response.keySet()) {
                int partitionConsumerFailCount = 0;
                long elapsedTime = DEFAULT_ELAPSED_TIME;
                try {
                    // Future.get() waits for task to get completed
                    elapsedTime = Long.valueOf(response.get(key).get());
                } catch (InterruptedException | ExecutionException e) {
                    m_logger.error("Error Reading from Topic: {}; Partition: {}; Exception: {}", item.topic(), key, e);
                }
                if (elapsedTime >= DEFAULT_ELAPSED_TIME) {
                    topicConsumerFailCount++;
                    partitionConsumerFailCount++;
                    if (isTopicAvailable) {
                        consumerFailCount++;
                        isTopicAvailable = false;
                    }
                }
                MetricNameEncoded consumerPartitionLatency = metricNameFactory.createWithPartition("Consumer.Latency", item.topic() + "##" + key);
                Histogram histogramConsumerPartitionLatency = new Histogram(new SlidingWindowReservoir(1));
                if (!metrics.getNames().contains(new Gson().toJson(consumerPartitionLatency))) {
                    if (appProperties.sendConsumerPartitionLatency)
                        metrics.register(new Gson().toJson(consumerPartitionLatency), histogramConsumerPartitionLatency);
                }
                histogramConsumerPartitionLatency.update(elapsedTime);
                histogramConsumerTopicLatency.update(elapsedTime);
                histogramConsumerLatency.update(elapsedTime);
                if (appProperties.sendConsumerPartitionAvailability) {
                    MetricNameEncoded consumerPartitionAvailability = metricNameFactory.createWithPartition("Consumer.Availability", item.topic() + "##" + key);
                    if (!metrics.getNames().contains(new Gson().toJson(consumerPartitionAvailability))) {
                        metrics.register(new Gson().toJson(consumerPartitionAvailability), new AvailabilityGauge(1, 1 - partitionConsumerFailCount));
                    }
                }
            }
            if (appProperties.sendConsumerTopicAvailability) {
                MetricNameEncoded consumerTopicAvailability = metricNameFactory.createWithTopic("Consumer.Availability", item.topic());
                if (!metrics.getNames().contains(new Gson().toJson(consumerTopicAvailability))) {
                    metrics.register(new Gson().toJson(consumerTopicAvailability), new AvailabilityGauge(response.keySet().size(), response.keySet().size() - topicConsumerFailCount));
                }
            }
        }

        if (appProperties.sendConsumerAvailability) {
            MetricNameEncoded consumerAvailability = metricNameFactory.createWithTopic("Consumer.Availability", "all");
            if (!metrics.getNames().contains(new Gson().toJson(consumerAvailability))) {
                metrics.register(new Gson().toJson(consumerAvailability), new AvailabilityGauge(consumerTryCount, consumerTryCount - consumerFailCount));
            }
        }

        ((MetaDataManager) metaDataManager).close();
        m_logger.info("Finished ConsumerLatency");
    }
}