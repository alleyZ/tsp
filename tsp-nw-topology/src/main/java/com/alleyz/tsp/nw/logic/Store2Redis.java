package com.alleyz.tsp.nw.logic;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alleyz.tsp.helper.RedisHelper;

import java.util.Map;

import static com.alleyz.tsp.constant.Constant.REDIS_NW_KEY;
import static com.alleyz.tsp.nw.constant.Constant.DEC_FREQ;
import static com.alleyz.tsp.nw.constant.Constant.DEC_WORD;
import static com.alleyz.tsp.nw.constant.Constant.TOPOLOGY_STREAM_NW_ID;

/**
 * Created by alleyz on 2017/5/31.
 *
 */
public class Store2Redis implements IBasicBolt {

    public final static String NAME = "new-word-store";
    @Override
    public void prepare(Map stormConf, TopologyContext context) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(TOPOLOGY_STREAM_NW_ID.equals(input.getSourceStreamId())){
            String word = input.getStringByField(DEC_WORD);
            String freq = input.getStringByField(DEC_FREQ);
            RedisHelper.getInstance().add2Hash(REDIS_NW_KEY, word, freq);
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
