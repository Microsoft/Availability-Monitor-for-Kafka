//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.threads;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingWindowReservoir;
import com.google.gson.Gson;
import com.microsoft.kafkaavailability.*;
import com.microsoft.kafkaavailability.discovery.CommonUtils;
import com.microsoft.kafkaavailability.metrics.AvailabilityGauge;
import com.microsoft.kafkaavailability.metrics.MetricNameEncoded;
import com.microsoft.kafkaavailability.metrics.MetricsFactory;
import com.microsoft.kafkaavailability.properties.AppProperties;
import com.microsoft.kafkaavailability.properties.ConsumerProperties;
import com.microsoft.kafkaavailability.properties.MetaDataManagerProperties;
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

public class ConsumerThread implements Runnable {

    final static Logger m_logger = LoggerFactory.getLogger(ConsumerThread.class);
    Phaser m_phaser;
    CuratorFramework m_curatorFramework;
    List<String> m_listServers;
    String m_serviceSpec;
    String m_clusterName;
    MetricsFactory metricsFactory;
    long m_threadSleepTime;

    public ConsumerThread(Phaser phaser, CuratorFramework curatorFramework, List<String> listServers, String serviceSpec, String clusterName, long threadSleepTime) {
        this.m_phaser = phaser;
        this.m_curatorFramework = curatorFramework;
        this.m_phaser.register(); //Registers/Add a new unArrived party to this phaser.
        CommonUtils.dumpPhaserState("After registration of ConsumerThread", phaser);
        m_listServers = listServers;
        m_serviceSpec = serviceSpec;
        m_clusterName = clusterName;
        this.m_threadSleepTime = threadSleepTime;
    }

    @Override
    public void run() {
        int sleepDuration = 1000;
        do {
            long lStartTime = System.nanoTime();
            MetricRegistry metrics;
            m_logger.info(Thread.currentThread().getName() +
                    " - Consumer party has arrived and is working in "
                    + "Phase-" + m_phaser.getPhase());

            try {
                metricsFactory = new MetricsFactory();
                metricsFactory.configure(m_clusterName);

                metricsFactory.start();
                metrics = metricsFactory.getRegistry();
                RunConsumer(metrics);
                metricsFactory.report();
                CommonUtils.sleep(1000);
            } catch (Exception e) {
                m_logger.error(e.getMessage(), e);
            } finally {
                try {
                    metricsFactory.stop();
                } catch (Exception e) {
                    m_logger.error(e.getMessage(), e);
                }
            }
            long elapsedTime = CommonUtils.stopWatch(lStartTime);
            m_logger.info("Consumer Elapsed: " + elapsedTime + " milliseconds.");

            while (elapsedTime < m_threadSleepTime && !m_phaser.isTerminated()) {
                try {
                    Thread.currentThread().sleep(sleepDuration);
                    elapsedTime = elapsedTime + sleepDuration;
                } catch (InterruptedException ie) {
                    m_logger.error(ie.getMessage(), ie);
                }
            }

            m_phaser.arriveAndDeregister();
            CommonUtils.dumpPhaserState("After arrival of ConsumerThread", m_phaser);

        } while (!m_phaser.isTerminated());
        m_logger.info("ConsumerThread (run()) has been COMPLETED.");
    }

    private void RunConsumer(MetricRegistry metrics) throws IOException, MetaDataManagerException {

        m_logger.info("Starting ConsumerLatency");

        IPropertiesManager consumerPropertiesManager = new PropertiesManager<ConsumerProperties>("consumerProperties.json", ConsumerProperties.class);
        IPropertiesManager metaDataPropertiesManager = new PropertiesManager<MetaDataManagerProperties>("metadatamanagerProperties.json", MetaDataManagerProperties.class);
        IMetaDataManager metaDataManager = new MetaDataManager(m_curatorFramework, metaDataPropertiesManager);

        IPropertiesManager appPropertiesManager = new PropertiesManager<AppProperties>("appProperties.json", AppProperties.class);
        AppProperties appProperties = (AppProperties) appPropertiesManager.getProperties();

        int numPartitionsConsumers = 0;
        //default to 15 Seconds, if not configured
        long consumerPartitionTimeoutInSeconds = (appProperties.consumerPartitionTimeoutInSeconds > 0 ? appProperties.consumerPartitionTimeoutInSeconds : 15);
        long consumerTopicTimeoutInSeconds = (appProperties.consumerTopicTimeoutInSeconds > 0 ? appProperties.consumerTopicTimeoutInSeconds : 45);

        //This is full list of topics
        List<kafka.javaapi.TopicMetadata> totalTopicMetadata = metaDataManager.getAllTopicPartition();
        List<kafka.javaapi.TopicMetadata> allTopicMetadata = new ArrayList<kafka.javaapi.TopicMetadata>();

        for (kafka.javaapi.TopicMetadata topic : totalTopicMetadata) {
            if ((totalTopicMetadata.indexOf(topic) % m_listServers.size()) == m_listServers.indexOf(m_serviceSpec)) {
                allTopicMetadata.add(topic);
            }
        }

        m_logger.info("totalTopicMetadata size:" + totalTopicMetadata.size());
        m_logger.info("allTopicMetadata size in Consumer:" + allTopicMetadata.size());

        int consumerTryCount = 0;
        int consumerFailCount = 0;

        for (kafka.javaapi.TopicMetadata topic : allTopicMetadata) {
            numPartitionsConsumers += topic.partitionsMetadata().size();
        }

        final SlidingWindowReservoir consumerLatencyWindow = new SlidingWindowReservoir(numPartitionsConsumers);
        Histogram histogramConsumerLatency = new Histogram(consumerLatencyWindow);
        MetricNameEncoded consumerLatency = new MetricNameEncoded("Consumer.Latency", "all");
        if (!metrics.getNames().contains(new Gson().toJson(consumerLatency))) {
            if (appProperties.sendConsumerLatency) {
                metrics.register(new Gson().toJson(consumerLatency), histogramConsumerLatency);
            }
        }
        for (kafka.javaapi.TopicMetadata item : allTopicMetadata) {
            m_logger.info("Reading from Topic: {};", item.topic());
            final SlidingWindowReservoir topicLatency = new SlidingWindowReservoir(item.partitionsMetadata().size());
            Histogram histogramConsumerTopicLatency = new Histogram(topicLatency);
            MetricNameEncoded consumerTopicLatency = new MetricNameEncoded("Consumer.Topic.Latency", item.topic());
            if (!metrics.getNames().contains(new Gson().toJson(consumerTopicLatency))) {
                if (appProperties.sendConsumerTopicLatency)
                    metrics.register(new Gson().toJson(consumerTopicLatency), histogramConsumerTopicLatency);
            }

            // check the number of available processors
            int nThreads = Runtime.getRuntime().availableProcessors();
            //Get ExecutorService from Executors utility class, thread pool size is number of available processors
            ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(nThreads);

            //create a list to hold the Future object associated with Callable
            //List<Future<Long>> futures = new ArrayList<Future<Long>>();
            Map<Integer, Future<Long>> response = new HashMap<Integer, Future<Long>>();

            for (kafka.javaapi.PartitionMetadata part : item.partitionsMetadata()) {
                m_logger.debug("Reading from Topic: {}; Partition: {};", item.topic(), part.partitionId());
                consumerTryCount++;

                //Create ConsumerPartitionThread instance
                ConsumerPartitionThread consumerPartitionJob = new ConsumerPartitionThread(m_curatorFramework, item, part);

                //submit Callable tasks to be executed by thread pool
                Future<Long> future = newFixedThreadPool.submit(new JobManager(consumerPartitionTimeoutInSeconds, TimeUnit.SECONDS, consumerPartitionJob));

                //add Future to the list, we can get return value using Future
                //futures.add(future);
                response.put(part.partitionId(), future);
            }

            //shut down the executor service now. This will make the executor accept no new threads
            // and finish all existing threads in the queue
            newFixedThreadPool.shutdown();

            try {
                // Wait until all threads are finish
                newFixedThreadPool.awaitTermination(consumerTopicTimeoutInSeconds, TimeUnit.SECONDS); //Global Timeout
            } catch (InterruptedException e) {
                m_logger.error("Error Reading from Topic: {}; Exception: {}", item.topic(), e);
            }

            int topicConsumerFailCount = 0;
            for (Integer key : response.keySet()) {
                long elapsedTime = DEFAULT_ELAPSED_TIME;
                try {
                    // Future.get() waits for task to get completed
                    elapsedTime = Long.valueOf(response.get(key).get());
                } catch (InterruptedException | ExecutionException e) {
                    m_logger.error("Error Reading from Topic: {}; Partition: {}; Exception: {}", item.topic(), key, e);
                }
                if (elapsedTime >= DEFAULT_ELAPSED_TIME) {
                    m_logger.error("Exception or Timeout during consuming from [{},{}]", item.topic(), key);
                    topicConsumerFailCount++;
                    consumerFailCount++;
                }
                MetricNameEncoded consumerPartitionLatency = new MetricNameEncoded("Consumer.Partition.Latency", item.topic() + "##" + key);
                Histogram histogramConsumerPartitionLatency = new Histogram(new SlidingWindowReservoir(1));
                if (!metrics.getNames().contains(new Gson().toJson(consumerPartitionLatency))) {
                    if (appProperties.sendConsumerPartitionLatency)
                        metrics.register(new Gson().toJson(consumerPartitionLatency), histogramConsumerPartitionLatency);
                }
                histogramConsumerPartitionLatency.update(elapsedTime);
                histogramConsumerTopicLatency.update(elapsedTime);
                histogramConsumerLatency.update(elapsedTime);
            }

            if (appProperties.sendConsumerAvailability) {
                MetricNameEncoded consumerAvailability = new MetricNameEncoded("Consumer.Availability", item.topic());
                if (!metrics.getNames().contains(new Gson().toJson(consumerAvailability))) {
                    metrics.register(new Gson().toJson(consumerAvailability), new AvailabilityGauge(response.keySet().size(), response.keySet().size() - topicConsumerFailCount));
                }
            }
        }

        if (appProperties.sendConsumerAvailability) {
            MetricNameEncoded consumerAvailability = new MetricNameEncoded("Consumer.Availability", "all");
            if (!metrics.getNames().contains(new Gson().toJson(consumerAvailability))) {
                metrics.register(new Gson().toJson(consumerAvailability), new AvailabilityGauge(consumerTryCount, consumerTryCount - consumerFailCount));
            }
        }

        ((MetaDataManager) metaDataManager).close();
        m_logger.info("Finished ConsumerLatency");
    }
}