package com.alleyz.tsp.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by alleyz on 2017/5/16.
 * 简单消费者
 */
public class SimpleProducer {
    public static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private String topic;
    private KafkaProducer<String, String> producer;
    public SimpleProducer(String topic, Properties prop) {
        logger.info("build producer topic is " + topic);
        this.topic = topic;
        this.producer = new KafkaProducer<>(prop);
    }

    /**
     * 发送消息
     * @param key 键
     * @param value 值
     * @param partition 分区
     * @param timestamp 时间戳
     */
    public void sendMsg(String key, String value, Integer partition, Long timestamp) {
        send(new ProducerRecord<>(this.topic, partition, timestamp, key, value));
    }
    public void sendMsg(String key, String value, Integer partition) {
        send(new ProducerRecord<>(this.topic, partition, key, value));
    }
    public void sendMsg(String key, String value) {
        send(new ProducerRecord<>(this.topic, key, value));
    }
    public void sendMsg(String value) {
        send(new ProducerRecord<>(this.topic, value));
    }

    /**
     * 发送消息
     * @param record {@link ProducerRecord}
     */
    private void send(ProducerRecord<String, String> record) {
        this.producer.send(record);
    }
}
