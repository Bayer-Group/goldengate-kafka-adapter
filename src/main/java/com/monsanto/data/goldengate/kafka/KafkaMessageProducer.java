package com.monsanto.data.goldengate.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.monsanto.data.goldengate.MessageProducer;
import com.monsanto.data.goldengate.config.KafkaConfiguration;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Enumeration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaMessageProducer implements MessageProducer {
    private final Logger log = LoggerFactory.getLogger(KafkaMessageProducer.class);
    private final String topicName;
    private final Producer<byte[], byte[]> producer ;
    private Optional<String> compressionFormat = Optional.absent();

    public KafkaMessageProducer(Producer<byte[], byte[]> producer, String topicName) {
        this.producer = producer;
        this.topicName = topicName;
    }

    public KafkaMessageProducer(String topicName, Producer<byte[], byte[]> producer, Optional<String> compressionFormat) {
        this.topicName = topicName;
        this.producer = producer;
        this.compressionFormat = compressionFormat;
    }

    public KafkaMessageProducer(KafkaConfiguration configuration) {
        log.info("Kafka Producer Constructor");
        this.topicName = configuration.getTopicName();
        this.producer = new KafkaProducer<>(createProperties(configuration.getBrokerList(),configuration.getCompression()));
    }

    @Override
    public void produce(byte[] bytes) throws UnableToSendMessageException {

        try {
            Future<RecordMetadata> futureMetadata =  producer.send(new ProducerRecord<byte[], byte[]>(topicName, bytes));
            RecordMetadata recordMetadata = futureMetadata.get(30000, TimeUnit.MILLISECONDS);

            if (log.isDebugEnabled() && recordMetadata != null) { // null check for UTs
                log.debug("Message Sent! offset=" + recordMetadata.offset() + ", topic=" + recordMetadata.topic() + ", partition=" + recordMetadata.partition());
            }
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            log.error("Error sending message to topic " + topicName + ": " + new String(bytes), e);
            throw new UnableToSendMessageException("Error sending kafka message",e);
        }
    }

    @Override
    public void terminate() {
        producer.close();
    }

    @VisibleForTesting
    public String getTopicName() {
        return topicName;
    }

    @VisibleForTesting
    public Optional<String> getCompressionFormat() {
        return compressionFormat;
    }

    private Properties createProperties(String brokerList, Optional<String> compressionCodec) {
        log.info("Creating Kafka ProducerConfig");
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,UUID.randomUUID().toString());

        if (compressionCodec.isPresent()) {
            properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,compressionCodec.get());
            compressionFormat = compressionCodec;
        }

        Enumeration keys = properties.keys();
        while (keys.hasMoreElements()) {
            String key = (String)keys.nextElement();
            String value = (String)properties.get(key);
            log.info(key + ": " + value);
        }
        return properties;
    }
}
