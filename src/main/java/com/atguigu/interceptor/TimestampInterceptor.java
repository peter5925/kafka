package com.atguigu.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author yhm
 * @create 2021-05-08 15:02
 *
 *  给发送的消息前面添加时间戳
 */
public class TimestampInterceptor implements ProducerInterceptor<String,String> {


    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 获取value值
        String value = record.value();

        // 添加时间戳
        String valueNew = System.currentTimeMillis() + value;

        // 创建一个新的producerRecord
        ProducerRecord<String, String> record1 = new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), valueNew, record.headers());

        return record1;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
