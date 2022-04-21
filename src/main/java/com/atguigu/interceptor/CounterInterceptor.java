package com.atguigu.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author yhm
 * @create 2021-05-08 15:08
 * <p>
 * 统计发送的消息中有多少条成功,多少条失败,最终打印出来
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {

    private int success;
    private int error;


    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {


        // 原信息返回,不进行修改
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {


        if (exception == null) {
            success += 1;
        } else {
            error += 1;
        }

        // 这里打印会造成发一条消息汇报一次结果
//        System.out.println("成功发送了:" + success + ",发送失败了:" + error);

    }

    @Override
    public void close() {
        // 在close方法当中进行打印,可以先收集结果,给出最终的结果进行打印
        System.out.println("成功发送了:" + success + ",发送失败了:" + error);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
