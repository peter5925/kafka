package com.atguigu.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 * @author yhm
 * @create 2021-05-08 16:03
 */
public class MyInterceptorToKafka implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        // 如果首位为字母,发送到first主题,
        // 如果是数字,发送到second,
        // 如果是其他,发送到third

        // 获取标志位
        byte b = event.getBody()[0];
        Map<String, String> headers = event.getHeaders();

        if (b >= '0' && b <= '9'){
            headers.put("topic","second");
        }
        if ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')){
            headers.put("topic","first");
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        // 循环遍历list
        for (Event event : events) {
            intercept(event);
        }

        return events;
    }

    @Override
    public void close() {

    }

    public static class MyBuilder implements Builder{

        @Override
        public Interceptor build() {
            return new MyInterceptorToKafka();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
