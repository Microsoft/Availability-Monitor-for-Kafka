package com.microsoft.kafkaavailability.metrics;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class MetricNameEncodedFactory {

    private final Map<String, String> commonDimensions;

    public MetricNameEncodedFactory(String clusterName, String machineName) {
        this.commonDimensions = ImmutableMap.of(
                DIMENSION_NAME_CLUSTER, clusterName,
                DIMENSION_NAME_MACHINE, machineName);
    }

    private static final String DIMENSION_NAME_CLUSTER = "Cluster";
    private static final String DIMENSION_NAME_MACHINE = "Machine";
    private static final String DIMENSION_NAME_TOPIC = "Topic";
    private static final String DIMENSION_NAME_PARTITION = "Partition";
    private static final String DIMENSION_NAME_VIP = "VIP";

    public MetricNameEncoded createWithDefaultDimensions(String metricName) {
        return new MetricNameEncoded(metricName, "", commonDimensions);
    }

    public MetricNameEncoded createWithVIP(String metricName, String vip) {
        Map<String, String> dimensions = ImmutableMap.<String, String>builder()
                .putAll(commonDimensions)
                .put(DIMENSION_NAME_VIP, vip)
                .build();

        return new MetricNameEncoded(metricName, vip, dimensions);
    }

    public MetricNameEncoded createWithTopic(String metricName, String topic) {
        Map<String, String> dimensions = ImmutableMap.<String, String>builder()
                .putAll(commonDimensions)
                .put(DIMENSION_NAME_TOPIC, topic)
                .build();

        return new MetricNameEncoded(metricName, topic, dimensions);
    }

    public MetricNameEncoded createWithPartition(String metricName, String partition) {
        Map<String, String> dimensions = ImmutableMap.<String, String>builder()
                .putAll(commonDimensions)
                .put(DIMENSION_NAME_PARTITION, partition)
                .build();

        return new MetricNameEncoded(metricName, partition, dimensions);
    }
}
