package com.xunmall.example.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;

/**
 * Created by wangyanjing on 2021/9/25.
 */
public class PrefixSource extends AbstractSource implements Configurable, PollableSource {

    private String prefix;
    private String subfix;

    @Override
    public Status process() throws EventDeliveryException {
        Event event = new SimpleEvent();

        HashMap<String, String> headers = new HashMap<>();

        try {
            for (int i = 0; i < 5; i++) {
                event.setHeaders(headers);
                event.setBody((prefix + "atuigu" + subfix).getBytes());
                getChannelProcessor().processEvent(event);
            }
            return Status.READY;
        } catch (Exception ex) {
            ex.printStackTrace();
            return Status.BACKOFF;
        }
        
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        prefix = context.getString("pre", "pre-");
        subfix = context.getString("sub", "sub-");
    }
}
