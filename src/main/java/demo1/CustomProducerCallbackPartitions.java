package demo1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerCallbackPartitions {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // 发送数据指定分区
        for (int i = 0; i < 5; i++) {
            ProducerRecord<String, String> first = new ProducerRecord<String, String>("first", 2, "", "zheji" + i);
            kafkaProducer.send(first, (meta, exception) -> {
                if (exception == null) {
                    System.out.println("主题: " + meta.topic() + "分区是: " + meta.partition());

                }
            });
        }
        kafkaProducer.close();
    }
}
