package com.monsanto.data.goldengate.kafka;

import com.google.common.base.Optional;
import com.monsanto.data.goldengate.config.KafkaConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class KafkaMessageProducer_UT {
    private static final String TOPIC_NAME = "foobar";

    @Mock
    Future<RecordMetadata> futureRecordMetadata;

    @Mock
    private KafkaProducer<byte[],byte[]> kafkaProducer;

    private KafkaMessageProducer kafkaMessageProducer;

    @Before
    public void setUp() {
        kafkaMessageProducer = new KafkaMessageProducer(kafkaProducer, TOPIC_NAME);
        Mockito.when(kafkaProducer.send(Matchers.<ProducerRecord<byte[], byte[]>>any())).thenReturn(futureRecordMetadata);
    }

    @Test
    public void sendsByteArrayToTopic() {
        final byte[] bytes = new byte[2];
        kafkaMessageProducer.produce(bytes);
        class MatchesOnBytes extends ArgumentMatcher<ProducerRecord<byte[], byte[]>> {
            public boolean matches(Object record) {
                return ((ProducerRecord) record).value() == bytes;
            }
        }

        verify(kafkaProducer).send(Matchers.argThat(new MatchesOnBytes()));
    }

    @Test
    public void canBeCreatedFromProcessConfiguration() {
        String brokerUri = "localhost:9092,localhost:9093";
        String topicName = "topic1";
        KafkaConfiguration configuration = new KafkaConfiguration(brokerUri, topicName, Optional.<String>absent());

        KafkaMessageProducer configuredProducer = new KafkaMessageProducer(configuration);

        assertThat(configuredProducer.getTopicName(), equalTo(topicName));
        assertThat(configuredProducer.getCompressionFormat(), equalTo(Optional.<String>absent()));
    }

    @Test
    public void setsCompressionMethodInProducerWhenPresent() {
        String brokerUri = "localhost:9092,localhost:9093";
        String topicName = "topic1";
        Optional<String> gzip = Optional.fromNullable("gzip");
        KafkaConfiguration configuration = new KafkaConfiguration(brokerUri, topicName, gzip);

        KafkaMessageProducer configuredProducer = new KafkaMessageProducer(configuration);

        assertThat(configuredProducer.getCompressionFormat(), equalTo(gzip));
    }

    @Test(expected = UnableToSendMessageException.class)
    public void throwsExceptionWhenKafkaProducerThrowsOne() throws Exception {
        ExecutionException e = new ExecutionException("blah",new IllegalArgumentException("blah"));
        doThrow(e).when(futureRecordMetadata).get(anyLong(), Matchers.<TimeUnit>any());

        kafkaMessageProducer.produce(new byte[2]);
    }

}
