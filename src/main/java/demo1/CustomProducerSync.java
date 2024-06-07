package demo1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CustomProducerSync {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 配置项
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 创建生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // 关于同步的发送数据
        for (int i = 0; i < 5; i++) {
            ProducerRecord producerRecord = new ProducerRecord("first", "zheji");
            kafkaProducer.send(producerRecord, (meta, exception) -> {
                if (exception == null) {
                    System.out.println("主题: " + meta.topic() + "分区:" + meta.partition());
                }
            }).get();
        }

        // 关闭连接
        kafkaProducer.close();
    }
}
