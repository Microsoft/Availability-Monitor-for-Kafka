package com.microsoft.kafkaavailability.module;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoMap;
import com.google.inject.multibindings.StringMapKey;
import com.google.inject.name.Names;
import com.microsoft.kafkaavailability.discovery.CommonUtils;
import com.microsoft.kafkaavailability.metrics.MetricNameEncodedFactory;
import com.microsoft.kafkaavailability.properties.AppProperties;
import com.microsoft.kafkaavailability.properties.ReporterProperties;
import com.microsoft.kafkaavailability.reporters.SqlReporter;
import com.microsoft.kafkaavailability.reporters.StatsdClient;
import com.microsoft.kafkaavailability.reporters.StatsdReporter;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class ReportersModule extends AbstractModule {

    public static final String ENVIRONMENT_NAME_CONSTANT_NAME = "environmentName";
    public static final String STATSD_ENDPOINT_CONSTANT_NAME = "statsdEndpoint";
    public static final String STATSD_PORT_CONSTANT_NAME = "statsdPort";
    public static final String METRICS_NAMESPACE_CONSTANT_NAME = "metricsNamespace";

    @Inject
    private AppProperties appProperties;
    @Inject
    private ReporterProperties reporterProperties;
    @Inject
    private MetricRegistry metricRegistry;

    @Override
    protected void configure() {
        bind(String.class).annotatedWith(Names.named(ENVIRONMENT_NAME_CONSTANT_NAME)).toInstance(appProperties.environmentName);
        bind(String.class).annotatedWith(Names.named(STATSD_ENDPOINT_CONSTANT_NAME)).toInstance(reporterProperties.statsdEndpoint);
        bind(Integer.class).annotatedWith(Names.named(STATSD_PORT_CONSTANT_NAME)).toInstance(reporterProperties.statsdPort);
        bind(String.class).annotatedWith(Names.named(METRICS_NAMESPACE_CONSTANT_NAME)).toInstance(reporterProperties.metricsNamespace);
    }

    @ProvidesIntoMap
    @StringMapKey("statsdReporter")
    public ScheduledReporter statsdReporter() {
        String endpoint = reporterProperties.statsdEndpoint == null ? "localhost" : reporterProperties.statsdEndpoint;

        final StatsdClient statsdClient = new StatsdClient(endpoint, reporterProperties.statsdPort);

        return StatsdReporter.forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.SECONDS)
                .build(statsdClient);
    }


    @ProvidesIntoMap
    @StringMapKey("sqlReporter")
    public ScheduledReporter sqlReporter() {

        String sqlConnection = reporterProperties.sqlConnectionString == null ? "localhost" : reporterProperties.sqlConnectionString;

        String clusterName = appProperties.environmentName == null ? "Unknown" : appProperties.environmentName;

        return SqlReporter.forRegistry(metricRegistry)
                .formatFor(Locale.US)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.SECONDS)
                .build(sqlConnection, clusterName);
    }


    @ProvidesIntoMap
    @StringMapKey("consoleReporter")
    public ScheduledReporter consoleReporter() {

        return ConsoleReporter
                .forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.SECONDS)
                .build();
    }

    @Provides
    @Singleton
    public MetricNameEncodedFactory metricNameEncodedFactory() {
        String localMachineName = CommonUtils.getComputerName();

        return new MetricNameEncodedFactory(appProperties.environmentName, localMachineName);
    }
}
