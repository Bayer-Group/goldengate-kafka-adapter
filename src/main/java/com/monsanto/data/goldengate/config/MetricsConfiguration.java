package com.monsanto.data.goldengate.config;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class MetricsConfiguration {
    private final Logger log = LoggerFactory.getLogger(MetricsConfiguration.class);
    private final Boolean enabled;
    private final String loggerName;
    private final Long reportFrequencyInSeconds;

    public MetricsConfiguration(Boolean enabled, String loggerName, Long reportFrequencyInSeconds) {
        log.info("MetricsConfiguration Constructor: enabled="+enabled+", loggerName="+loggerName+", reportFrequencyInSeconds="+ reportFrequencyInSeconds);
        this.enabled = enabled;
        this.loggerName = loggerName;
        this.reportFrequencyInSeconds = reportFrequencyInSeconds;
    }

    public Boolean isEnabled() {
        return enabled;
    }

    public String getLoggerName() {
        return loggerName;
    }

    public Long getReportFrequencyInSeconds() {
        return reportFrequencyInSeconds;
    }

    public static MetricsConfiguration fromConfig(Config config) {
        return new MetricsConfiguration(config.getBoolean("enabled"), config.getString("logger-name"), config.getDuration("report-frequency", TimeUnit.SECONDS));
    }
}
