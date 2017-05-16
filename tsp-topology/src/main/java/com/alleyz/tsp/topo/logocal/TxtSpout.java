package com.alleyz.tsp.topo.logocal;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alleyz.tsp.kafka.consumer.SimpleConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static com.alleyz.tsp.constant.Constant.DELIMITER_BLOCK;
import static com.alleyz.tsp.constant.Constant.TXT_MSG_GROUP_TOPOLOGY;
import static com.alleyz.tsp.constant.Constant.TXT_MSG_TOPIC;
import static com.alleyz.tsp.topo.constant.TopoConstant.*;

/**
 * Created by alleyz on 2017/5/16.
 * 接受kafka数据
 */
public class TxtSpout implements IRichSpout{
    private static final Logger logger = LoggerFactory.getLogger(TxtSpout.class);
    private SpoutOutputCollector collector;
    private SimpleConsumer consumer;
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        Properties props = (Properties) conf.get(KEY_TOPOLOGY_CONSUMER_PROPS);
        Long interval = (Long) conf.get(KEY_TOPOLOGY_CONSUMER_INTERVAL);
        if(interval == null) interval = 1000L;
        this.consumer = new SimpleConsumer(TXT_MSG_TOPIC, TXT_MSG_GROUP_TOPOLOGY, props, interval);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(
                TOPOLOGY_STREAM_TXT_ID,
                new Fields(DEC_ROW_KEY, DEC_BASIC_INFO, DEC_USER_TXT, DEC_AGENT_TXT, DEC_ALL_TXT)
        );
    }

    @Override
    public void nextTuple() {
        this.consumer.pollAndProcessMsg(crs -> {
            Iterator<ConsumerRecord<String, String>> iterator = crs.iterator();
            while (iterator.hasNext()){
                ConsumerRecord<String, String> record = iterator.next();
                String rowKey = record.key();
                String value = record.value();
                String[] values = value.split(DELIMITER_BLOCK);
                logger.debug("tuple rowKey is " + rowKey);
                this.collector.emit(
                        TOPOLOGY_STREAM_TXT_ID,
                        new Values(
                                rowKey,
                                values[0],
                                values[1],
                                values[2],
                                values[3]
                ));
            }
        });

    }



    @Override
    public void close() {
        this.consumer.close();
    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }


    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
