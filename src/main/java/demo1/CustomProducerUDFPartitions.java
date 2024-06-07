package demo1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class CustomProducerUDFPartitions {
    public static void main(String[] args) {
        // 配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 指定分区器,参数是全类名
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "demo1.MyPartitions");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 20; i++) {
            ProducerRecord<String, String> first = new ProducerRecord<>("first", "zhejiya" + i);
            kafkaProducer.send(first, (meta, exception)->{
                if (exception == null){
                    System.out.println("主题: " + meta.topic() + "分区: " + meta.partition());
                }
            });

        }
        kafkaProducer.close();
    }
}
