package com.alleyz.tsp.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collections;
import java.util.Properties;

/**
 * Created by alleyz on 2017/5/16.
 *
 */
public class SimpleConsumer implements Closeable{
    private static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private long pollInterval;
    private KafkaConsumer<String, String> consumer;
    public SimpleConsumer(String topic, String group, Properties prop, Long pollInterval) {
        this.pollInterval = pollInterval;

        //如果代码中设置组，则代码中组优先
        if(!(group == null || "".equals(group))){
            prop.put("group.id", group);
        }

        this.consumer = new KafkaConsumer<>(prop);
        this.consumer.subscribe(Collections.singletonList(topic));
        this.consumer.metrics();
    }

    /**
     * 接受处理消息
     * @param handler 消息处理类
     */
    public void pollAndProcessMsg(MsgHandler handler){
//        while (true) {
            logger.debug("AutoConsumer - startAccept: 接受消息");
            ConsumerRecords<String, String> crs = this.consumer.poll(pollInterval);
            if(crs.isEmpty()) return;
            handler.process(crs);
            this.consumer.commitSync();
//        }
    }

    public void close() {
        this.consumer.close();
    }


    /**
     * 消息处理类
     */
    public interface MsgHandler {
        void process(ConsumerRecords<String, String> crs);
    }

}
