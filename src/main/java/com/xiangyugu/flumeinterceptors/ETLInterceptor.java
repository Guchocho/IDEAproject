package com.xiangyugu.flumeinterceptors;

import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class ETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //获取body，编码成字符串
        String body = new String(event.getBody(), StandardCharsets.UTF_8);
        //2.按照Json格式解析字符串
        JsonParser parser = new JsonParser();
        try {
            JsonElement jsonElement = parser.parse(body);
            return event;
        } catch (JsonParseException e){
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        //1.遍历数据
        Iterator<Event> iterator = list.iterator();
        while (iterator.hasNext()){
            if (intercept(iterator.next()) == null) {
                iterator.remove();
            }
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class build implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}