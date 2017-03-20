//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.metrics;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.microsoft.kafkaavailability.IPropertiesManager;
import com.microsoft.kafkaavailability.PropertiesManager;
import com.microsoft.kafkaavailability.properties.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class MetricsFactory implements IMetricsFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsFactory.class);
    //private static final MetricsFactory INSTANCE = new MetricsFactory();

    private final MetricRegistry metricRegistry;
    private List<ConfiguredReporter> reporters = Collections.synchronizedList(new LinkedList<ConfiguredReporter>());

    /**
     * A reporter that knows how to start and stop itself with a pre-configured
     * reporting period.
     */
    private static class ConfiguredReporter {
        private ScheduledReporter reporter;
        private long period;

        /**
         * Create a new instance.
         *
         * @param reporter the reporter
         * @param period   period in milliseconds. Values less than zero mean that the reporter
         *                 should not be scheduled, but may be used ad hoc through
         *                 report function.
         */
        public ConfiguredReporter(ScheduledReporter reporter, long period) {
            this.reporter = reporter;
            this.period = period;
        }

        /**
         * Start the reporter (if it has a regular period).
         */
        public void start() {
            if (period > 0) {
                reporter.start(period, TimeUnit.MILLISECONDS);
            }
        }

        /**
         * Stop the reporter.
         */
        public void stop() {
            reporter.stop();
        }

        /**
         * Immediately send a report on the metrics.
         */
        public void report() {
            reporter.report();
        }
    }

    /**
     * Singleton access functions, but with package level access only for
     * testing.
     */
    public MetricsFactory() {
        this(new MetricRegistry());
    }

    /**
     * Singleton access functions, but with package level access only for
     * testing
     *
     * @param registry the registry for use.
     */
    public MetricsFactory(MetricRegistry registry) {
        metricRegistry = registry;
    }

    /**
     * Configure the instance.
     *
     * @throws Exception
     */
    @Override
    public void configure(String clusterName) throws Exception {
        LOGGER.debug("Configuring metrics");

        stop();
        reporters.clear();

        IPropertiesManager appPropertiesManager = new PropertiesManager<AppProperties>("appProperties.json", AppProperties.class);
        AppProperties appProperties = (AppProperties) appPropertiesManager.getProperties();
        String sqlConnectionString = appProperties.sqlConnectionString;
        Integer period = appProperties.reportInterval;

        Map<String, Object> config = new HashMap<>();
        config.put("sqlConnectionString", sqlConnectionString);
        config.put("period", period);
        config.put("cluster", clusterName);

        ScheduledReporter consoleReporter = ReporterUtils.createConsoleReporter(metricRegistry, config);
        reporters.add(new ConfiguredReporter(consoleReporter, period * 1000));

        ScheduledReporter sqlReporter = ReporterUtils.createSqlReporter(metricRegistry, config);
        reporters.add(new ConfiguredReporter(sqlReporter, period * 1000));

        // Install the logging listener (probably a configuration item)
        metricRegistry.addListener(new LoggingMetricListener());

        LOGGER.info("Metrics have been configured");
    }

    /**
     * Get the underlying metrics registry.
     *
     * @return
     */
    public MetricRegistry getRegistry() {
        return metricRegistry;
    }

    /**
     * Get or create a metric counter, with default naming.
     *
     * @param clazz
     * @param name
     * @return
     */
    public Counter getCounter(Class<?> clazz, String name) {
        return metricRegistry.counter(makeName(clazz, name));
    }

    /**
     * Get or create a metric meter, with default naming.
     *
     * @param clazz
     * @param name
     * @return
     */
    public Meter getMeter(Class<?> clazz, String name) {
        return metricRegistry.meter(makeName(clazz, name));
    }

    /**
     * Get or create a metric histogram, with default naming.
     *
     * @param clazz
     * @param name
     * @return
     */
    public Histogram getHistogram(Class<?> clazz, String name) {
        return metricRegistry.histogram(makeName(clazz, name));
    }

    /**
     * Get or create a metric timer, with default naming.
     *
     * @param clazz
     * @param name
     * @return
     */
    public Timer getTimer(Class<?> clazz, String name) {
        return metricRegistry.timer(makeName(clazz, name));
    }

    /**
     * Get or create a metric counter, with default naming.
     *
     * @param base
     * @param name
     * @return
     */
    public Counter getCounter(String base, String name) {
        return metricRegistry.counter(makeName(base, name));
    }

    /**
     * Get or create a metric meter, with default naming.
     *
     * @param base
     * @param name
     * @return
     */
    public Meter getMeter(String base, String name) {
        return metricRegistry.meter(makeName(base, name));
    }

    /**
     * Get or create a metric histogram, with default naming.
     *
     * @param base
     * @param name
     * @return
     */
    public Histogram getHistogram(String base, String name) {
        return metricRegistry.histogram(makeName(base, name));
    }

    /**
     * Get or create a metric timer, with default naming.
     *
     * @param base
     * @param name
     * @return
     */
    public Timer getTimer(String base, String name) {
        return metricRegistry.timer(makeName(base, name));
    }

    /**
     * Create a name using the default scheme.
     *
     * @param base
     * @param name
     * @return
     */
    public String makeName(String base, String name) {
        return base + Metrics.SEP + name;
    }

    /**
     * Create a name using the default scheme.
     *
     * @param clazz
     * @param name
     * @return
     */
    public String makeName(Class<?> clazz, String name) {
        return makeName(clazz.getCanonicalName(), name);
    }

    @Override
    public void start() {
        LOGGER.debug("Starting metrics");
        // Start the reporters
        synchronized (reporters) {
            Iterator<ConfiguredReporter> reporterIterator = reporters.listIterator();
            while (reporterIterator.hasNext()) {
                reporterIterator.next().start();
            }
        }
    }

    @Override
    public void stop() {
        LOGGER.debug("Stopping metrics");
        synchronized (reporters) {
            Iterator<ConfiguredReporter> reporterIterator = reporters.listIterator();
            while (reporterIterator.hasNext()) {
                reporterIterator.next().stop();
            }
        }

        removeAll();
    }

    /**
     * Remove all metrics from the registry
     */
    public void removeAll() {
        getRegistry().removeMatching(new MetricFilter() {
            @Override
            public boolean matches(String arg0, Metric arg1) {
                return true;
            }
        });
    }

    /**
     * Force send metrics to the reporters (out of scheduled time)
     */
    public void report() {
        for (ConfiguredReporter reporter : reporters) {
            reporter.report();
        }
    }
}