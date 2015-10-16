package com.monsanto.data.goldengate.kafka;

import com.monsanto.data.goldengate.MessageProducer;
import com.monsanto.data.goldengate.config.KafkaConfiguration;

public class KafkaProducerFactory {

    public static MessageProducer create(KafkaConfiguration configuration) {

        // TODO: Extract the broker configuration here
        return new KafkaMessageProducer(configuration);
    }
}
