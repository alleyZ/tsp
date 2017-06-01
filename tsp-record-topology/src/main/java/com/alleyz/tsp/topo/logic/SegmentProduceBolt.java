package com.alleyz.tsp.topo.logic;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alleyz.tsp.config.ConfigUtils;
import com.alleyz.tsp.kafka.producer.SimpleProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

import static com.alleyz.tsp.constant.Constant.DELIMITER_BLOCK;
import static com.alleyz.tsp.constant.Constant.DELIMITER_FIELDS;
import static com.alleyz.tsp.constant.Constant.SEG_MSG_TOPIC;
import static com.alleyz.tsp.topo.constant.TopoConstant.*;

/**
 * Created by alleyz on 2017/5/22.
 * 负责将结果发送至kafka， 不再直接上hdfs
 */
public class SegmentProduceBolt implements IBasicBolt {
    public static final String NAME = "segment-produce-bolt";
    private static Logger logger = LoggerFactory.getLogger(SegmentProduceBolt.class);
    private SimpleProducer producer;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        Properties prop = ConfigUtils.getProp("/producer.properties");
        this.producer = new SimpleProducer(SEG_MSG_TOPIC, prop);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(TOPOLOGY_STREAM_SEG_WORD_ID.equals(input.getSourceStreamId())){
            String day = input.getStringByField(DEC_DAY),
                    prov = input.getStringByField(DEC_PROVINCE),
                    rowKey = input.getStringByField(DEC_ROW_KEY),
                    segTxt = input.getStringByField(DEC_SEG_WORD);
            String content = day + DELIMITER_BLOCK + prov + DELIMITER_FIELDS + segTxt + "\r\n";
            try {
                producer.sendMsg(rowKey, content);
            }catch (Exception e) {
                logger.error("send segment msg  has error " + rowKey, e);
            }
        }
    }


    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
