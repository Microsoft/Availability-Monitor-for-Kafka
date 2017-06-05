package com.microsoft.kafkaavailability.reporters;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * A reporter which publishes metric values to a Statds server.
 *
 * @see <a href="https://github.com/etsy/statsd">StatsdClient</a>
 */
public class StatsdReporter extends ScheduledReporter {

    /**
     * Returns a new {@link Builder} for {@link StatsdReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link StatsdReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link StatsdReporter} instances. Defaults to not using a prefix, using the
     * default clock, converting rates to events/second, converting durations to milliseconds, and
     * not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private String prefix;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.prefix = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
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
         * Builds a {@link StatsdReporter} with the given properties, sending metrics using the
         * given {@link StatsdClient} client.
         *
         * @param statsdClient a {@link StatsdClient} client
         * @return a {@link StatsdReporter}
         */
        public StatsdReporter build(StatsdClient statsdClient) {
            return new StatsdReporter(registry,
                    statsdClient,
                    prefix,
                    filter,
                    rateUnit,
                    durationUnit);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(StatsdReporter.class);

    private final StatsdClient statsdClient;
    private final String prefix;

    public StatsdReporter(MetricRegistry registry,
                          StatsdClient statsdClient,
                          String prefix,
                          MetricFilter filter,
                          TimeUnit rateUnit,
                          TimeUnit durationUnit) {
        super(registry, "statsdClient-reporter", filter, rateUnit, durationUnit);

        this.statsdClient = statsdClient;
        this.prefix = prefix;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {

        try {
            LOGGER.info("Start reporting");
            statsdClient.connect();

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
                reportMetered(entry.getKey(), entry.getValue());
            }

            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                reportTimer(entry.getKey(), entry.getValue());
            }
            LOGGER.info("done reporting");

        } catch(IOException e) {
            LOGGER.warn("Unable to report to StatsD", statsdClient, e);
        } finally {
            try {
                statsdClient.close();
            } catch (IOException e) {
                LOGGER.debug("Error disconnecting from StatsD server", statsdClient, e);
            }
        }
    }

    private void reportTimer(String name, Timer timer) throws IOException {
        final Snapshot snapshot = timer.getSnapshot();

        statsdClient.send(prefix(name, "max"),
                format(convertDuration(snapshot.getMax())),
                StatsdClient.StatType.TIMER);
        statsdClient.send(prefix(name, "mean"),
                format(convertDuration(snapshot.getMean())),
                StatsdClient.StatType.TIMER);

        statsdClient.send(prefix(name, "p95"),
                format(convertDuration(snapshot.get95thPercentile())),
                StatsdClient.StatType.TIMER);

    }

    private void reportMetered(String name, Metered meter) throws IOException {
        statsdClient.send(prefix(name, "count"), format(meter.getCount()), StatsdClient.StatType.GAUGE);

        statsdClient.send(prefix(name, "mean_rate"),
                format(convertRate(meter.getMeanRate())),
                StatsdClient.StatType.TIMER);
    }

    private void reportHistogram(String name, Histogram histogram) throws IOException {
        final Snapshot snapshot = histogram.getSnapshot();
        statsdClient.send(prefix(name, "count"),
                format(histogram.getCount()),
                StatsdClient.StatType.GAUGE);
        statsdClient.send(prefix(name, "max"),
                format(snapshot.getMax()),
                StatsdClient.StatType.TIMER);
        statsdClient.send(prefix(name, "mean"),
                format(snapshot.getMean()),
                StatsdClient.StatType.TIMER);

        statsdClient.send(prefix(name, "p95"),
                format(snapshot.get95thPercentile()),
                StatsdClient.StatType.TIMER);

    }

    private void reportCounter(String name, Counter counter) throws IOException {
        statsdClient.send(prefix(name, "count"),
                format(counter.getCount()),
                StatsdClient.StatType.COUNTER);
    }

    private void reportGauge(String name, Gauge gauge) throws IOException {
        final String value = format(gauge.getValue());
        if (value != null) {
            statsdClient.send(prefix(name), value,
                    StatsdClient.StatType.GAUGE);
        }
    }

    private String format(Object o) {
        if (o instanceof Float) {
            return format(((Float) o).doubleValue());
        } else if (o instanceof Double) {
            return format(((Double) o).doubleValue());
        } else if (o instanceof Byte) {
            return format(((Byte) o).longValue());
        } else if (o instanceof Short) {
            return format(((Short) o).longValue());
        } else if (o instanceof Integer) {
            return format(((Integer) o).longValue());
        } else if (o instanceof Long) {
            return format(((Long) o).longValue());
        }
        return null;
    }

    private String prefix(String... components) {
        return MetricRegistry.name(prefix, components);
    }

    private String format(long n) {
        return Long.toString(n);
    }

    private String format(double v) {
        return String.format(Locale.US, "%2.2f", v);
    }
}