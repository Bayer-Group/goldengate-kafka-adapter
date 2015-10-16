package com.monsanto.data.goldengate;

public interface MessageProducer {

    void produce(byte[] bytes);

    void terminate();

}
