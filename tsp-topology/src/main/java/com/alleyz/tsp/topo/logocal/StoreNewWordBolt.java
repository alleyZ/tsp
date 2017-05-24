package com.alleyz.tsp.topo.logocal;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alleyz.tsp.constant.Constant;
import com.alleyz.tsp.topo.utils.RedisHelper;

import java.util.Map;

import static com.alleyz.tsp.constant.Constant.DELIMITER_FIELDS;
import static com.alleyz.tsp.topo.constant.TopoConstant.*;

/**
 * Created by alleyz on 2017/5/16.
 *
 */
public class StoreNewWordBolt implements IBasicBolt{
    public final static String NAME = "store-new-word-bolt";
    @Override
    public void prepare(Map stormConf, TopologyContext context) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(TOPOLOGY_STREAM_NEW_WORD_ID.equals(input.getSourceStreamId())) {
            String word = input.getStringByField(DEC_NW_WORD),
                    nature = input.getStringByField(DEC_NW_WORD_NATURE),
                    weight = input.getStringByField(DEC_NW_WORD_WEIGHT),
                    freq = input.getStringByField(DEC_NW_WORD_FREQ);
            RedisHelper.getInstance().add2Hash(Constant.REDIS_KEY_NEW_WORD, word, nature + DELIMITER_FIELDS
                    + weight + DELIMITER_FIELDS + freq);
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
