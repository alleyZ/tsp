package com.alleyz.tsp.topo.logocal;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alleyz.tsp.config.ConfigUtils;
import com.alleyz.tsp.kafka.consumer.SimpleConsumer;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static com.alleyz.tsp.constant.Constant.*;
import static com.alleyz.tsp.topo.constant.TopoConstant.*;

/**
 * Created by alleyz on 2017/5/16.
 * 接受kafka数据
 */
public class TxtSpout implements IRichSpout{
    public static final String NAME = "txt-spout";
    private static final Logger logger = LoggerFactory.getLogger(TxtSpout.class);
    private SpoutOutputCollector collector;
    private SimpleConsumer consumer;
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        Properties props = ConfigUtils.getProp("/consumer.properties");
        Long interval = ConfigUtils.getLongVal("poll.interval.mills", 1000L);
        this.consumer = new SimpleConsumer(TXT_MSG_TOPIC, TXT_MSG_GROUP_TOPOLOGY, props, interval);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(
                TOPOLOGY_STREAM_TXT_ID,
                new Fields(DEC_ROW_KEY, DEC_PROVINCE, DEC_DAY,
                        DEC_BASIC_INFO, DEC_USER_TXT, DEC_AGENT_TXT, DEC_ALL_TXT)
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
                String basicInfo = values[0];
                String[] txt = values[1].split(DELIMITER_FIELDS);
                logger.info("++++++++++ tuple rowKey is " + rowKey);
                try {
                    this.collector.emit(
                            TOPOLOGY_STREAM_TXT_ID,
                            new Values(
                                    rowKey,
                                    rowKey.substring(10, 12),
                                    StringUtils.reverse(rowKey.substring(0, 10))
                                            .substring(0, 8),
                                    basicInfo,
                                    txt[0],
                                    txt[1],
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
