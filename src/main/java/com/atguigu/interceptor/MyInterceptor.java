package com.atguigu.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author yhm
 * @create 2021-05-08 14:33
 */
public class MyInterceptor implements ProducerInterceptor {

    /**
     * 在消息发送之前对消息进行处理
     * @param record
     * @return
     */
    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        return record;
    }

    /**
     * 在回调函数执行之前对数据进行处理
     * @param metadata
     * @param exception
     */
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
