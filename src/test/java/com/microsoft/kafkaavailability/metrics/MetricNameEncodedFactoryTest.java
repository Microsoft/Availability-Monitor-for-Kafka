package com.microsoft.kafkaavailability.metrics;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MetricNameEncodedFactoryTest {
    private static final String DIMENSION_NAME_CLUSTER = "Cluster";
    private static final String DIMENSION_NAME_MACHINE = "Machine";
    private static final String DIMENSION_NAME_TOPIC = "Topic";
    private static final String DIMENSION_NAME_PARTITION = "Partition";
    private static final String DIMENSION_NAME_VIP = "VIP";

    private static final String CLUSTER = "CLUSTER";
    private static final String MACHINE = "MACHINE";
    private static final String VIP = "VIP";
    private static final String TOPIC = "TOPIC";
    private static final String PARTITION = "PARTITION";
    private static final String METRIC_NAME = "METRIC_NAME";

    private final MetricNameEncodedFactory factory = new MetricNameEncodedFactory(CLUSTER, MACHINE);

    @Test
    public void createWithDefaultDimensions() {
        MetricNameEncoded expected = new MetricNameEncoded(METRIC_NAME, "",
                ImmutableMap.of(DIMENSION_NAME_CLUSTER, CLUSTER, DIMENSION_NAME_MACHINE, MACHINE));
        MetricNameEncoded actual = factory.createWithDefaultDimensions(METRIC_NAME);

        assertEquals(expected, actual);
    }

    @Test
    public void createWithVIP() throws Exception {
        MetricNameEncoded expected = new MetricNameEncoded(METRIC_NAME, VIP,
                ImmutableMap.of(DIMENSION_NAME_CLUSTER, CLUSTER,
                        DIMENSION_NAME_MACHINE, MACHINE,
                        DIMENSION_NAME_VIP, VIP));
        MetricNameEncoded actual = factory.createWithVIP(METRIC_NAME, VIP);

        assertEquals(expected, actual);
    }

    @Test
    public void createWithTopic() throws Exception {
        MetricNameEncoded expected = new MetricNameEncoded(METRIC_NAME, TOPIC,
                ImmutableMap.of(DIMENSION_NAME_CLUSTER, CLUSTER,
                        DIMENSION_NAME_MACHINE, MACHINE,
                        DIMENSION_NAME_TOPIC, TOPIC));
        MetricNameEncoded actual = factory.createWithTopic(METRIC_NAME, TOPIC);

        assertEquals(expected, actual);
    }

    @Test
    public void createWithPartition() throws Exception {
        MetricNameEncoded expected = new MetricNameEncoded(METRIC_NAME, PARTITION,
                ImmutableMap.of(DIMENSION_NAME_CLUSTER, CLUSTER,
                        DIMENSION_NAME_MACHINE, MACHINE,
                        DIMENSION_NAME_PARTITION, PARTITION));
        MetricNameEncoded actual = factory.createWithPartition(METRIC_NAME, PARTITION);

        assertEquals(expected, actual);
    }

}