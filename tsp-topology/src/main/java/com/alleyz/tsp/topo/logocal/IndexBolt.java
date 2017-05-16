package com.alleyz.tsp.topo.logocal;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TupleHelpers;
import com.alleyz.tsp.topo.constant.TopoConstant;

import java.util.Map;

/**
 * Created by alleyz on 2017/5/16.
 *
 */
public class IndexBolt implements IBasicBolt{

    @Override
    public void prepare(Map stormConf, TopologyContext context) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(TupleHelpers.isTickTuple(input)) {
            return;
        }
        if(TopoConstant.TOPOLOGY_STREAM_TXT_ID.equals(input.getSourceStreamId())) {
            String rowKey = input.getStringByField(TopoConstant.DEC_ROW_KEY);
            String basicInfo = input.getStringByField(TopoConstant.DEC_BASIC_INFO);
            String userTxt = input.getStringByField(TopoConstant.DEC_AGENT_TXT);
            String allTxt = input.getStringByField(TopoConstant.DEC_ALL_TXT);

        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
