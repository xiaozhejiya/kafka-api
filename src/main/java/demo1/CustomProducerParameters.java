package demo1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerParameters {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 指定缓冲区大小
        // 32M
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // 指定批次大小
        // 16K
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 16);
        // 指定linger.ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // 压缩
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // 指定分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "demo1.MyPartitions");
        // 创建生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // 发送数据
        for (int i = 0; i < 5; i++) {
            ProducerRecord<String, String> first = new ProducerRecord("first", "zheji" + i);
            kafkaProducer.send(first);
        }
        kafkaProducer.close();
    }
}
