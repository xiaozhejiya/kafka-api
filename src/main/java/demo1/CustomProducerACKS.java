package demo1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerACKS {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // 设置发送节点
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        // 设置序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024);
        // 设置批次大小
        // 16k
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16 * 1024);
        // 设置是否压缩
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // 设置发送时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // 设置ack模式
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        // 设置重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        // 创建kafka生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        // 发送数据
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first", "zheji" + i);
            kafkaProducer.send(producerRecord,(meta, exception)->{
                if (exception == null){
                    System.out.println("主题为: " + meta.topic() + "分区为:" + meta.partition());
                }
            });
        }
        // 关闭连接
        kafkaProducer.close();
    }
}
