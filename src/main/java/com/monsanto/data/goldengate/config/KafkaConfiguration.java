package com.monsanto.data.goldengate.config;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConfiguration {
    private final Logger log = LoggerFactory.getLogger(KafkaConfiguration.class);
    private final String brokerList;
    private final String topicName;
    private final Optional<String> compression;

    public KafkaConfiguration(String brokerList, String topicName, Optional<String> compression) {
        log.info("KafkaConfiguration Constructor: brokerList="+brokerList+", topicName="+topicName+", compression="+ compression);
        this.brokerList = brokerList;
        this.topicName = topicName;
        this.compression = compression;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public String getTopicName() {
        return topicName;
    }

    public Optional<String> getCompression() {
        return compression;
    }

    public static KafkaConfiguration fromConfig(Config config) {

        Optional<String> possibleCompression;
        if (config.hasPath("compression")) {
            possibleCompression = Optional.of(config.getString("compression"));
        } else {
            possibleCompression = Optional.absent();
        }
        return new KafkaConfiguration(config.getString("broker-list"), config.getString("topic-name"), possibleCompression);
    }
}
