package com.alleyz.tsp.nw.logic;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alleyz.tsp.config.ConfigUtils;
import com.alleyz.tsp.kafka.consumer.SimpleConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static com.alleyz.tsp.constant.Constant.*;
import static com.alleyz.tsp.nw.constant.Constant.DEC_TXT;
import static com.alleyz.tsp.nw.constant.Constant.TOPOLOGY_STREAM_NW_SPOUT_ID;

/**
 * Created by alleyz on 2017/5/31.
 *
 */
public class TxtSpout implements IRichSpout{
    public static final String NAME = "nw-txt-spout";
    private static final Logger logger = LoggerFactory.getLogger(TxtSpout.class);
    private SpoutOutputCollector collector;
    private SimpleConsumer consumer;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        Properties props = ConfigUtils.getProp("/consumer.properties");
        Long interval = ConfigUtils.getLongVal("poll.interval.mills", 1000L);
        String group = (String)conf.get(MSG_GROUP);
        this.consumer = new SimpleConsumer(TXT_MSG_TOPIC, group == null ? TXT_MSG_GROUP_TOPOLOGY : group,
                props, interval);
    }

    @Override
    public void nextTuple() {
        this.consumer.pollAndProcessMsg(crs -> {
            Iterator<ConsumerRecord<String, String>> iterator = crs.iterator();
            while (iterator.hasNext()){
                ConsumerRecord<String, String> record = iterator.next();
                String value = record.value();
                try {
                    String[] values = value.split(DELIMITER_BLOCK);
                    String[] txt = values[1].split(DELIMITER_FIELDS);
                    this.collector.emit(
                            TOPOLOGY_STREAM_NW_SPOUT_ID,
                            new Values(
                                    txt[2]
                            ));

                }catch (Exception e){
                    logger.error("txt-spout has err", e);
                }
            }
            return true;
        });
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(
                TOPOLOGY_STREAM_NW_SPOUT_ID,
                new Fields(DEC_TXT)
        );
    }

    @Override
    public void close() {

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
