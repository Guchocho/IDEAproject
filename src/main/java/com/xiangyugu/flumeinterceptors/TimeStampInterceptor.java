package com.xiangyugu.flumeinterceptors;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/*
根据body记录的时间戳添加Header
 */
public class TimeStampInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //1.获取body和header
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody(), StandardCharsets.UTF_8);
        //2.获取body中的时间戳
        JsonParser jsonParser = new JsonParser();
        JsonElement element = jsonParser.parse(body);

        if (element.isJsonObject()){
            String timestamp = element.getAsJsonObject().get("ts").getAsString();
            headers.put("timestamp",timestamp);
        } else {
            return null;
        }
        //3.添加到header里
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
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

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TimeStampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
