package io.eventador;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;

public class FlinkReadWriteKafka {
    public static void main(String[] args) throws Exception {
        // Read parameters from command line
        final ParameterTool params = ParameterTool.fromArgs(args);

        if(params.getNumberOfParameters() < 4) {
            System.out.println("\nUsage: FlinkReadWriteKafka --read-topic <topic> --write-topic <topic> --bootstrap.servers <kafka brokers> --group.id <groupid>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(300000); // 300 seconds
        env.getConfig().setGlobalJobParameters(params);

        String readTopic = params.getRequired("read-topic");
        String writeTopic = params.getRequired("write-topic");

        DataStreamSource<KafkaMessage> messageStream = env
                .addSource(new FlinkKafkaConsumer<>(
                        readTopic,
                        new KafkaDeserializationSchema<KafkaMessage>() {
                            @Override
                            public boolean isEndOfStream(KafkaMessage kafkaMessage) {
                                return false;
                            }

                            @Override
                            public KafkaMessage deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                                return new KafkaMessage(
                                        consumerRecord.key(),
                                        consumerRecord.value(),
                                        consumerRecord.topic(),
                                        consumerRecord.partition(),
                                        consumerRecord.timestamp(),
                                        consumerRecord.headers());
                            }

                            @Override
                            public TypeInformation<KafkaMessage> getProducedType() {
                                return TypeInformation.of(KafkaMessage.class);
                            }
                        },
                        params.getProperties()));
         // Print Kafka messages to stdout - will be visible in logs
        messageStream.print();

        // If you want to perform some transformations before writing the data
        // back to Kafka, do it here!

        // Write payload back to Kafka topic
        messageStream
                .addSink(new FlinkKafkaProducer<KafkaMessage>(
                                writeTopic,
                                new KafkaSerializationSchema<KafkaMessage>() {
                                    @Override
                                    public ProducerRecord<byte[], byte[]> serialize(KafkaMessage msg, @Nullable Long aLong) {
                                        return new ProducerRecord<>(
                                                writeTopic,
                                                msg.partition,
                                                msg.timestamp,
                                                msg.key,
                                                msg.value,
                                                msg.headers);
                                    }
                                },
                                params.getProperties(),
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
                .name("Write To Kafka");

        env.execute("FlinkReadWriteKafka");
    }
}
