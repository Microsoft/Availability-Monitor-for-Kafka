//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.metrics;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
 
/** * Helper utility class to capture the creation of the different reporter types. 
 * See each {@link MetricsFactory} for configuration details and examples. 
 * 
 *  
 * 
 */ 
public class ReporterUtils {

    private ReporterUtils() {

    }

    /**
     * Create a new console reporter.
     *
     * @param metricRegistry the registry to report on
     * @param config         the configuration map (see {@link MetricsFactory})
     * @return the reporter instance
     */
    public static ScheduledReporter createConsoleReporter(MetricRegistry metricRegistry, Map<String, Object> config) {

        return ConsoleReporter.forRegistry(metricRegistry).convertRatesTo(getRatesUnit(config))
                .convertDurationsTo(getDurationUnit(config)).build();
    }

    /**
     * Create a new Sql reporter.
     *
     * @param metricRegistry the registry to report on
     * @param config         the configuration map (see {@link MetricsFactory})
     * @return the reporter instance
     */
    public static ScheduledReporter createSqlReporter(MetricRegistry metricRegistry, Map<String, Object> config) throws IOException {

        String sqlConnectionString = (String) config.get("sqlConnectionString");
        if (sqlConnectionString == null) {
            sqlConnectionString = "localhost";
        }

        String cluster = (String) config.get("cluster");
        if (cluster == null) {
            cluster = "Unknown";
        }

        return SqlReporter.forRegistry(metricRegistry)
                .formatFor(Locale.US)
                .convertRatesTo(getRatesUnit(config))
                .convertDurationsTo(getDurationUnit(config)).build(sqlConnectionString, cluster);

    }

    private static TimeUnit getDurationUnit(Map<String, Object> config) {
        String durationUnit = (String) config.get("durationUnit");
        if (durationUnit == null) {
            durationUnit = TimeUnit.SECONDS.name();
        }

        return TimeUnit.valueOf(durationUnit.toUpperCase());
    }

    private static TimeUnit getRatesUnit(Map<String, Object> config) {
        String rateUnit = (String) config.get("rateUnit");
        if (rateUnit == null) {
            rateUnit = TimeUnit.SECONDS.name();
        }
        return TimeUnit.valueOf(rateUnit.toUpperCase());
    }
}