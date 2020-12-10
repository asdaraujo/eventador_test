package io.eventador;

import org.apache.kafka.common.header.Headers;

public class KafkaMessage {
    byte[] key;
    byte[] value;

    String topic;
    int partition;
    long timestamp;

    Headers headers;

    KafkaMessage() {}
    KafkaMessage(byte[] key, byte[] value, String topic, int partition, long timestamp, Headers headers) {
        this.key = key;
        this.value = value;
        this.topic = topic;
        this.partition = partition;
        this.timestamp = timestamp;
        this.headers = headers;
    }
}
