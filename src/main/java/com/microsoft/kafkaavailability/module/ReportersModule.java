package com.microsoft.kafkaavailability.module;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.multibindings.ProvidesIntoMap;
import com.google.inject.multibindings.StringMapKey;
import com.google.inject.name.Names;
import com.microsoft.kafkaavailability.properties.AppProperties;
import com.microsoft.kafkaavailability.properties.ReporterProperties;
import com.microsoft.kafkaavailability.reporters.SqlReporter;
import com.microsoft.kafkaavailability.reporters.StatsdClient;
import com.microsoft.kafkaavailability.reporters.StatsdReporter;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class ReportersModule extends AbstractModule {

    @Inject
    private AppProperties appProperties;
    @Inject
    private ReporterProperties reporterProperties;
    @Inject
    private MetricRegistry metricRegistry;

    @Override
    protected void configure() {
        bind(String.class).annotatedWith(Names.named("environmentName")).toInstance(appProperties.environmentName);
        bind(String.class).annotatedWith(Names.named("statsdEndpoint")).toInstance(reporterProperties.statsdEndpoint);
        bind(Integer.class).annotatedWith(Names.named("statsdPort")).toInstance(reporterProperties.statsdPort);
        bind(String.class).annotatedWith(Names.named("metricsNamespace")).toInstance(reporterProperties.metricsNamespace);
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
}
