package com.alleyz.tsp.topo.logocal;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alleyz.tsp.topo.constant.TopoConstant;

import java.util.Map;

/**
 * Created by alleyz on 2017/5/16.
 *
 */
public class StoreOracleBolt implements IBasicBolt {
    public static final String NAME = "oraBolt";
    @Override
    public void prepare(Map stormConf, TopologyContext context) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (TopoConstant.TOPOLOGY_STREAM_QC_ID.equals(input.getSourceStreamId())){
            String rowKey = input.getStringByField(TopoConstant.DEC_ROW_KEY);
            String qcItem = input.getStringByField(TopoConstant.DEC_QC_ITEM);

            System.out.println(qcItem + " ===== " + rowKey);
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
