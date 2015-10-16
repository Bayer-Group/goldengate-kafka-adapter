package com.monsanto.data.goldengate.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import com.monsanto.data.goldengate.config.MetricsConfiguration;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class MetricsReporterFactory {

    public static ScheduledReporter createReporter(MetricsConfiguration configuration, MetricRegistry metrics) {

        ScheduledReporter reporter = Slf4jReporter.forRegistry(metrics)
                .outputTo(LoggerFactory.getLogger(configuration.getLoggerName()))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        reporter.start(configuration.getReportFrequencyInSeconds(), TimeUnit.SECONDS);

        return reporter;
    }

}
