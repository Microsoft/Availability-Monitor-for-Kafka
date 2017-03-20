//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.metrics;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.google.gson.Gson;
import com.microsoft.kafkaavailability.sql.JdbcConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.microsoft.kafkaavailability.discovery.CommonUtils.getWaitTimeExp;

/**
 * A reporter which sends the measurements for each metric to a SQL Database.
 */
public class SqlReporter extends ScheduledReporter {
    private static final Logger m_logger = LoggerFactory.getLogger(SqlReporter.class);

    private JdbcConnectionPool poolMgr;

    protected void initialize() throws SQLException {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); // Check that server's Java
            // has SQLServer support.
        } catch (final ClassNotFoundException e) {
            throw new SQLException("Can't load JDBC Driver", e);
        }

        //Instantiate an implementation of a com.microsoft.sqlserver.jdbc.SQLServerXADataSource
        com.microsoft.sqlserver.jdbc.SQLServerXADataSource dataSource = new com.microsoft.sqlserver.jdbc.SQLServerXADataSource();
        dataSource.setURL(connectionString);
        poolMgr = new JdbcConnectionPool(dataSource, 10);
    }

    /**
     * @param registry the registry to report
     * @return a {@link Builder}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /***
     * @param gauges     map of gauge names and objects
     * @param counters   map of counter names and objects
     * @param histograms map of histogram names and objects
     * @param meters     map with of meter and objects
     * @param timers     map with of timer and objects
     */
    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        try {
            for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                reportGauge(entry.getKey(), entry.getValue());
            }

            for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                reportCounter(entry.getKey(), entry.getValue());
            }

            for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
                reportHistogram(entry.getKey(), entry.getValue());
            }

            for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                reportMeter(entry.getKey(), entry.getValue());
            }

            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                reportTimer(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            m_logger.error(e.getMessage(), e);
        }
    }

    /**
     * A builder for  instances. Defaults to using the default locale, converting
     * rates to events/second, converting durations to milliseconds, and not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private Locale locale;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.locale = Locale.getDefault();
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        /**
         * Format numbers for the given {@link Locale}.
         *
         * @param locale a {@link Locale}
         * @return {@code this}
         */
        public Builder formatFor(Locale locale) {
            this.locale = locale;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }


        /**
         *
         *
         * @param connectionString
         * @return a {@link SqlReporter}
         */
        /***
         * Builds a {@link SqlReporter} with the given properties
         *
         * @param connectionString the connectionString for the db
         * @param userId           typically the cluster name where the service is running
         * @return {@link SqlReporter}
         */
        public SqlReporter build(String connectionString, String userId) {
            return new SqlReporter(registry,
                    connectionString,
                    userId,
                    locale,
                    rateUnit,
                    durationUnit,
                    filter);
        }
    }

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private final String connectionString;
    private final String userId;

    private final Locale locale;

    private SqlReporter(MetricRegistry registry,
                        String connectionString,
                        String userId,
                        Locale locale,
                        TimeUnit rateUnit,
                        TimeUnit durationUnit,
                        MetricFilter filter) {
        super(registry, "sql-reporter", filter, rateUnit, durationUnit);
        this.connectionString = connectionString;
        this.userId = userId;
        this.locale = locale;
        try {
            initialize();
        } catch (Exception e) {
            m_logger.error(e.getMessage(), e);
        }
    }


    private void reportTimer(String name, Timer timer) {
        final Snapshot snapshot = timer.getSnapshot();

        report(name,
                "count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit",
                "'%d','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','calls/%s','%s'",
                timer.getCount(),
                convertDuration(snapshot.getMax()),
                convertDuration(snapshot.getMean()),
                convertDuration(snapshot.getMin()),
                convertDuration(snapshot.getStdDev()),
                convertDuration(snapshot.getMedian()),
                convertDuration(snapshot.get75thPercentile()),
                convertDuration(snapshot.get95thPercentile()),
                convertDuration(snapshot.get98thPercentile()),
                convertDuration(snapshot.get99thPercentile()),
                convertDuration(snapshot.get999thPercentile()),
                convertRate(timer.getMeanRate()),
                convertRate(timer.getOneMinuteRate()),
                convertRate(timer.getFiveMinuteRate()),
                convertRate(timer.getFifteenMinuteRate()),
                getRateUnit(),
                getDurationUnit());
    }

    private void reportMeter(String name, Meter meter) {
        report(name,
                "count,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit",
                "'%d','%f','%f','%f','%f','events/%s'",
                meter.getCount(),
                convertRate(meter.getMeanRate()),
                convertRate(meter.getOneMinuteRate()),
                convertRate(meter.getFiveMinuteRate()),
                convertRate(meter.getFifteenMinuteRate()),
                getRateUnit());
    }

    private void reportHistogram(String name, Histogram histogram) {
        final Snapshot snapshot = histogram.getSnapshot();

        report(name,
                "count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999",
                "'%d','%d','%f','%d','%f','%f','%f','%f','%f','%f','%f'",
                histogram.getCount(),
                snapshot.getMax(),
                snapshot.getMean(),
                snapshot.getMin(),
                snapshot.getStdDev(),
                snapshot.getMedian(),
                snapshot.get75thPercentile(),
                snapshot.get95thPercentile(),
                snapshot.get98thPercentile(),
                snapshot.get99thPercentile(),
                snapshot.get999thPercentile());
    }

    private void reportCounter(String name, Counter counter) {
        report(name, "count", "'%d'", counter.getCount());
    }

    private void reportGauge(String name, Gauge gauge) {
        report(name, "value", "'%s'", gauge.getValue());
    }

    private void report(String name, String header, String line, Object... values) {
        Connection con = null;
        Statement stmt = null;
        int iMaxWaitInterval = 1800000;
        int iMaxRetries = 10;

        MetricNameEncoded metricNameEncoded = new Gson().fromJson(name, MetricNameEncoded.class);
        final long timestamp = metricNameEncoded.timeInSeconds();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            int retries = 0;

            do {

                try {
                    con = poolMgr.getConnection();
                    // Create and execute an SQL statement that returns some data.
                    String SQL = String.format(String.format("insert into [dbo].[%s] values('%s','%s','%s',%s)", metricNameEncoded.name, userId, sdf.format(new Date(timestamp * 1000)), metricNameEncoded.tag, line), values);

                    if (null != con) {
                        stmt = con.createStatement();
                        if (null != stmt) {
                            stmt.execute(SQL);
                            break;
                        }
                    }
                } catch (java.sql.SQLException e) {
                    logSQLException(e);
                } catch (Exception e) {
                    m_logger.error(e.getMessage(), e);
                } finally {
                    // Clean up the command and Database Connection objects
                    try {
                        if (stmt != null) stmt.close();
                    } catch (java.sql.SQLException e) {
                        logSQLException(e);
                    }

                    // Clean up Connection object
                    try {
                        if (con != null) con.close();
                    } catch (java.sql.SQLException e) {
                        logSQLException(e);
                    }
                    con = null;
                }
                long waitTime = Math.min(getWaitTimeExp(retries, 1000L), iMaxWaitInterval);
                // Sleep and continue (convert to milliseconds)
                Thread.sleep(waitTime);
            } while (retries++ < iMaxRetries);

        }
        // unreported exception java.lang.InterruptedException; must be caught or declared to be thrown
        catch (Exception ex) {
            ex.printStackTrace();
            m_logger.error(ex.getMessage(), ex);
        }
    }

    // Display an SQLException which has occured in this application.
    private static void logSQLException(java.sql.SQLException e) {
        // Notice that a SQLException is actually a chain of SQLExceptions,
        // let's print all of them...
        java.sql.SQLException next = e;
        while (next != null) {
            m_logger.error(next.getMessage() + " Error Code: " + next.getErrorCode() + " SQL State: " + next.getSQLState());
            next = next.getNextException();
        }
    }
}