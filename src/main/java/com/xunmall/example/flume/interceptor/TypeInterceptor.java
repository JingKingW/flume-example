package com.xunmall.example.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wangyanjing on 2021/9/25.
 */
public class TypeInterceptor implements Interceptor {

    private List<Event> addInterceptorEvents;

    @Override
    public void initialize() {
        addInterceptorEvents = new ArrayList<>();
    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();

        String body = new String(event.getBody());

        if (body.contains("atguigu")){
            headers.put("type","atguigu");
        }else {
            headers.put("type","other");
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        addInterceptorEvents.clear();

        for (Event event : list){
            addInterceptorEvents.add(intercept(event));
        }

        return addInterceptorEvents;
    }

    @Override
    public void close() {

    }

     public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
