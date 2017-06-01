package com.alleyz.tsp.nw.logic;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.coordination.IBatchBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by alleyz on 2017/5/31.
 *
 */
public class Store2Oracle implements IBatchBolt {
    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
//        collector.
    }

    @Override
    public void execute(Tuple tuple) {

    }

    @Override
    public void finishBatch() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
