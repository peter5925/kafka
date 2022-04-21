package com.atguigu.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author yhm
 * @create 2021-05-08 10:03
 */
public class MyProducer {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        // 1. 创建生产者的配置对象
        Properties properties = new Properties();

        // 2. 给配置对象添加参数
        properties.put("bootstrap.servers", "hadoop102:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

        // key和value的序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 非必要参数 调优
        // 设置ack
        properties.put("acks", "all");

        // 重试次数
        properties.put("retries", 3);

        // 批次大小 默认16K
        properties.put("batch.size", 16384);

        // 等待时间
        properties.put("linger.ms", 1);

        // RecordAccumulator缓冲区大小 默认32M
        properties.put("buffer.memory", 33554432);


        // 3. 创建kafka的生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        // 4. 调用send方法发送消息
        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>("first", "hello kunzong" + i));


//            Thread.sleep(100);
        }

        // 同步发送消息,不推荐使用
        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>("first", "hello kunzong" + i)).get();
        }


        // 5. 关闭资源
        producer.close();
    }
}
