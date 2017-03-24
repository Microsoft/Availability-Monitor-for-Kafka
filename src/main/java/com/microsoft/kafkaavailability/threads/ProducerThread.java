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
import com.microsoft.kafkaavailability.properties.MetaDataManagerProperties;
import com.microsoft.kafkaavailability.properties.ProducerProperties;
import kafka.javaapi.TopicMetadata;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.Phaser;

import static com.microsoft.kafkaavailability.discovery.Constants.DEFAULT_ELAPSED_TIME;

public class ProducerThread implements Callable<Long> {

    final static Logger m_logger = LoggerFactory.getLogger(ProducerThread.class);
    Phaser m_phaser;
    CuratorFramework m_curatorFramework;
    MetricsFactory metricsFactory;
    long m_threadSleepTime;
    String m_clusterName;

    public ProducerThread(Phaser phaser, CuratorFramework curatorFramework, long threadSleepTime, String clusterName) {
        this.m_phaser = phaser;
        this.m_curatorFramework = curatorFramework;
        //this.m_phaser.register(); //Registers/Add a new unArrived party to this phaser.
        //CommonUtils.dumpPhaserState("After register", phaser);
        this.m_threadSleepTime = threadSleepTime;
        this.m_clusterName = clusterName;
    }

    @Override
    public Long call() throws Exception {
        int sleepDuration = 1000;
        long elapsedTime = 0L;

        do {
            long lStartTime = System.nanoTime();
            MetricRegistry metrics;
            m_logger.info(Thread.currentThread().getName() +
                    " - Producer party has arrived and is working in "
                    + "Phase-" + m_phaser.getPhase());

            try {
                metricsFactory = new MetricsFactory();
                metricsFactory.configure(m_clusterName);

                metricsFactory.start();
                metrics = metricsFactory.getRegistry();
                RunProducer(metrics);

            } catch (Exception e) {
                m_logger.error(e.getMessage(), e);
            } finally {
                try {
                    metricsFactory.report();
                    CommonUtils.sleep(1000);
                    metricsFactory.stop();
                } catch (Exception e) {
                    m_logger.error(e.getMessage(), e);
                }
            }

            elapsedTime = CommonUtils.stopWatch(lStartTime);
            m_logger.info("Producer Elapsed: " + elapsedTime + " milliseconds.");

            while (elapsedTime < m_threadSleepTime && !m_phaser.isTerminated()) {
                try {
                    Thread.currentThread().sleep(sleepDuration);
                    elapsedTime = elapsedTime + sleepDuration;
                } catch (InterruptedException ie) {
                    m_logger.error(ie.getMessage(), ie);
                }
            }
            //phaser.arriveAndAwaitAdvance();
        } while (!m_phaser.isTerminated());
        m_logger.info("ProducerThread (run()) has been COMPLETED.");
        return Long.valueOf(elapsedTime);
    }

    private void RunProducer(MetricRegistry metrics) throws IOException, MetaDataManagerException {

        m_logger.info("Starting ProducerLatency");
        IPropertiesManager producerPropertiesManager = new PropertiesManager<ProducerProperties>("producerProperties.json", ProducerProperties.class);
        IPropertiesManager metaDataPropertiesManager = new PropertiesManager<MetaDataManagerProperties>("metadatamanagerProperties.json", MetaDataManagerProperties.class);
        IMetaDataManager metaDataManager = new MetaDataManager(m_curatorFramework, metaDataPropertiesManager);
        MetaDataManagerProperties metaDataProperties = (MetaDataManagerProperties) metaDataPropertiesManager.getProperties();

        IProducer producer = new Producer(producerPropertiesManager, metaDataManager);

        IPropertiesManager appPropertiesManager = new PropertiesManager<AppProperties>("appProperties.json", AppProperties.class);
        AppProperties appProperties = (AppProperties) appPropertiesManager.getProperties();

        int producerTryCount = 0;
        int producerFailCount = 0;
        long startTime, endTime;
        int numPartitionsProducer = 0;

        //Auto creating a white listed topics, if not available.
        metaDataManager.createCanaryTopics();

        //This is full list of topics
        List<TopicMetadata> totalTopicMetadata = metaDataManager.getAllTopicPartition();

        List<kafka.javaapi.TopicMetadata> whiteListTopicMetadata = new ArrayList<TopicMetadata>();

        for (kafka.javaapi.TopicMetadata topic : totalTopicMetadata) {
            for (String whiteListTopic : metaDataProperties.canaryTestTopics)
                // java string compare while ignoring case
                if (topic.topic().equalsIgnoreCase(whiteListTopic)) {
                    whiteListTopicMetadata.add(topic);
                }
        }

        m_logger.info("totalTopicMetadata size:" + totalTopicMetadata.size());
        m_logger.info("canaryTestTopicsMetadata size:" + whiteListTopicMetadata.size());

        for (kafka.javaapi.TopicMetadata topic : whiteListTopicMetadata) {
            numPartitionsProducer += topic.partitionsMetadata().size();
        }

        final SlidingWindowReservoir producerLatencyWindow = new SlidingWindowReservoir(numPartitionsProducer);
        Histogram histogramProducerLatency = new Histogram(producerLatencyWindow);

        MetricNameEncoded producerLatency = new MetricNameEncoded("Producer.Latency", "all");
        if (!metrics.getNames().contains(new Gson().toJson(producerLatency))) {
            if (appProperties.sendProducerLatency)
                metrics.register(new Gson().toJson(producerLatency), histogramProducerLatency);
        }

        m_logger.info("start topic partition loop");

        for (kafka.javaapi.TopicMetadata item : whiteListTopicMetadata) {
            boolean isTopicAvailable = true;
            int topicProducerFailCount = 0;
            producerTryCount++;
            final SlidingWindowReservoir topicLatency = new SlidingWindowReservoir(item.partitionsMetadata().size());
            Histogram histogramProducerTopicLatency = new Histogram(topicLatency);
            MetricNameEncoded producerTopicLatency = new MetricNameEncoded("Producer.Topic.Latency", item.topic());
            if (!metrics.getNames().contains(new Gson().toJson(producerTopicLatency))) {
                if (appProperties.sendProducerTopicLatency)
                    metrics.register(new Gson().toJson(producerTopicLatency), histogramProducerTopicLatency);
            }

            for (kafka.javaapi.PartitionMetadata part : item.partitionsMetadata()) {
                m_logger.debug("Writing to Topic: {}; Partition: {};", item.topic(), part.partitionId());
                MetricNameEncoded producerPartitionLatency = new MetricNameEncoded("Producer.Partition.Latency", item.topic() + "##" + part.partitionId());
                Histogram histogramProducerPartitionLatency = new Histogram(new SlidingWindowReservoir(1));
                if (!metrics.getNames().contains(new Gson().toJson(producerPartitionLatency))) {
                    if (appProperties.sendProducerPartitionLatency)
                        metrics.register(new Gson().toJson(producerPartitionLatency), histogramProducerPartitionLatency);
                }
                startTime = System.currentTimeMillis();
                try {

                    producer.SendCanaryToTopicPartition(item.topic(), Integer.toString(part.partitionId()));
                    endTime = System.currentTimeMillis();
                } catch (Exception e) {
                    m_logger.error("Error Writing to Topic: {}; Partition: {}; Exception: {}", item.topic(), part.partitionId(), e);
                    topicProducerFailCount++;
                    endTime = System.currentTimeMillis() + DEFAULT_ELAPSED_TIME;
                    if (isTopicAvailable) {
                        producerFailCount++;
                        isTopicAvailable = false;
                    }
                }
                histogramProducerLatency.update(endTime - startTime);
                histogramProducerTopicLatency.update(endTime - startTime);
                histogramProducerPartitionLatency.update(endTime - startTime);
            }
            if (appProperties.sendProducerTopicAvailability) {
                MetricNameEncoded producerTopicAvailability = new MetricNameEncoded("Producer.Topic.Availability", item.topic());
                if (!metrics.getNames().contains(new Gson().toJson(producerTopicAvailability))) {
                    metrics.register(new Gson().toJson(producerTopicAvailability), new AvailabilityGauge(item.partitionsMetadata().size(), item.partitionsMetadata().size() - topicProducerFailCount));
                }
            }
        }
        if (appProperties.sendProducerAvailability) {
            MetricNameEncoded producerAvailability = new MetricNameEncoded("Producer.Availability", "all");
            if (!metrics.getNames().contains(new Gson().toJson(producerAvailability))) {
                metrics.register(new Gson().toJson(producerAvailability), new AvailabilityGauge(producerTryCount, producerTryCount - producerFailCount));
            }
        }
        producer.close();
        ((MetaDataManager) metaDataManager).close();
        m_logger.info("Finished ProducerLatency");
    }
}