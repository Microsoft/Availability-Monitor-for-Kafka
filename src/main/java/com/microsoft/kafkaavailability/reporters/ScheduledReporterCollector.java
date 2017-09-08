//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.reporters;

import com.codahale.metrics.*;
import com.google.inject.Inject;
import com.microsoft.kafkaavailability.metrics.LoggingMetricListener;
import com.microsoft.kafkaavailability.properties.AppProperties;
import com.microsoft.kafkaavailability.properties.ReporterProperties;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class ScheduledReporterCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduledReporterCollector.class);
    private static final int DEFAULT_REPORT_INTERVAL_IN_SECONDS = 60;

    private final MetricRegistry metricRegistry;

    private int reportIntervalInSeconds = DEFAULT_REPORT_INTERVAL_IN_SECONDS;
    private final List<ScheduledReporter> reporters;

    @Inject
    public ScheduledReporterCollector(AppProperties appProperties, ReporterProperties reporterProperties,
                                      MetricRegistry metricRegistry,
                                      Map<String, ScheduledReporter> allReporters) throws Exception {
        LOGGER.debug("Configuring metrics");

        Integer period = appProperties.reportInterval;
        this.reportIntervalInSeconds = period > 0 ? period : DEFAULT_REPORT_INTERVAL_IN_SECONDS;


        reporters = selectReporters(reporterProperties, allReporters);

        //This is the singleton metric registry shared across the board
        this.metricRegistry = metricRegistry;
        // Install the logging listener (probably a configuration item)
        this.metricRegistry.addListener(new LoggingMetricListener());

        LOGGER.info("Reporters have been configured");
    }

    private List<ScheduledReporter> selectReporters(ReporterProperties reporterProperties, Map<String, ScheduledReporter> allReporters) {
        List<ScheduledReporter> reportersToUse;

        if(StringUtils.isBlank(reporterProperties.reportersCommaSeparatedList)) {
            reportersToUse = new ArrayList<>(allReporters.values());
        } else {
            reportersToUse = new ArrayList<>();
            List<String> reporterNames = Arrays.asList(reporterProperties.reportersCommaSeparatedList.split(","));
            for(String name : reporterNames) {
               if(allReporters.containsKey(name)) {
                   LOGGER.debug(name + " is added to ScheduledReporterCollector.");
                   reportersToUse.add(allReporters.get(name));
               }
            }
        }

        return Collections.synchronizedList(reportersToUse);
    }

    /**
     * Get the underlying metrics registry.
     *
     * @return
     */
    public MetricRegistry getRegistry() {
        return metricRegistry;
    }

    public void start() {
        LOGGER.debug("Starting metrics");
        // Start the reporters
        synchronized (reporters) {
            Iterator<ScheduledReporter> reporterIterator = reporters.listIterator();
            while (reporterIterator.hasNext()) {
                reporterIterator.next().start(reportIntervalInSeconds, TimeUnit.SECONDS);
            }
        }
    }

    public void stop() {
        LOGGER.debug("Stopping metrics");
        synchronized (reporters) {
            Iterator<ScheduledReporter> reporterIterator = reporters.listIterator();
            while (reporterIterator.hasNext()) {
                reporterIterator.next().stop();
            }
        }
    }

    /**
     * Force send metrics to the reporters (out of scheduled time)
     */
    public void report() {
        for (ScheduledReporter reporter : reporters) {
            try {
                reporter.report();
            } catch (Exception e) {
                LOGGER.error("Failed to report using " + reporter.getClass().getName(), e);
            }
        }
    }
}