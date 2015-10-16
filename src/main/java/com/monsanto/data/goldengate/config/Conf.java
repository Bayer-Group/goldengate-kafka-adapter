package com.monsanto.data.goldengate.config;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.monsanto.data.goldengate.encoder.EncoderType;

import java.util.Map;

public class Conf {
    private final EncoderType encoderType;
    private final KafkaConfiguration kafkaConfiguration;
    private final MetricsConfiguration metricsConfiguration;
    private final Map<String, Map<String, TableConfiguration>> tableConfigsByName = Maps.newHashMap();
    private final Map<String, TableConfiguration> tableNameToConfigMap = Maps.newHashMap();

    public Conf(EncoderType encoderType, KafkaConfiguration kafkaConfiguration, MetricsConfiguration metricsConfiguration) {
        this.encoderType = encoderType;
        this.kafkaConfiguration = kafkaConfiguration;
        this.metricsConfiguration = metricsConfiguration;
    }

    public EncoderType encoderType() {
        return encoderType;
    }

    public KafkaConfiguration kafka() {
        return kafkaConfiguration;
    }

    public boolean metricsEnabled() {
        return metricsConfiguration.isEnabled();
    }

    public MetricsConfiguration metrics() {
        return metricsConfiguration;
    }

    public void addTableConfiguration(TableConfiguration tableConfiguration) {
        tableNameToConfigMap.put(tableConfiguration.getName().toUpperCase(), tableConfiguration);
        tableConfigsByName.put(tableConfiguration.getSchema().toUpperCase(),tableNameToConfigMap);
    }

    public Optional<TableConfiguration> getTableConfiguration(String schemaName, String tableName) {
        Optional opt = Optional.fromNullable(tableConfigsByName.get(schemaName.toUpperCase()));
        if (opt.isPresent()) {
            return Optional.fromNullable(tableConfigsByName.get(schemaName.toUpperCase()).get(tableName.toUpperCase()));
        } else {
            return Optional.absent();
        }
    }

}
