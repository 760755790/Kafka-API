package Producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * 消息生产者
 *      1、不带回调函数
 */
public class CustomProducer {

    // 要注意kafka客户端版本要和kafka服务版本一致
    public static void main(String[] args) throws Exception{
        String  topic = "004";

        Properties prop = new Properties();
        // kafka集群，broker-list
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.159.132:9092");
        // ack状态
        prop.put(ProducerConfig.ACKS_CONFIG, "all");
        // 重试次数
        prop.put(ProducerConfig.RETRIES_CONFIG, 1);
        // 批次大小
        prop.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // 16k 大于16k发出去
        // 等待时间
        prop.put(ProducerConfig.LINGER_MS_CONFIG,1); // 1s
        // RecordAccumulator缓冲区大小
        prop.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>(topic,"测试" + i));
        }

        // 将消息发出去，不写的话 等待时间<1s 且批次大小<16K是发不出去的
        producer.close();


    }

}
