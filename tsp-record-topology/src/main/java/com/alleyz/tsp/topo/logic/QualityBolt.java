package com.alleyz.tsp.topo.logic;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alleyz.tsp.topo.constant.TopoConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.alleyz.tsp.topo.constant.TopoConstant.*;

/**
 * Created by alleyz on 2017/5/16.
 *
 */
public class QualityBolt implements IBasicBolt{
    private static Logger logger = LoggerFactory.getLogger(QualityBolt.class);
    public static final String NAME = "quaBolt";
    private Map<String, String> qcItems = new HashMap<>();

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        qcItems.put("国际漫游", "国际漫游");
        qcItems.put("来电提醒", "来电提醒");
        qcItems.put("投诉", "投诉");
        qcItems.put("信号问题", "信号不好");

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            if (TopoConstant.TOPOLOGY_STREAM_HBASE_ID.equals(input.getSourceStreamId())) {
                String allTxt = input.getStringByField(DEC_ALL_TXT);
                String rowKey = input.getStringByField(DEC_ROW_KEY);
                String prov = input.getStringByField(DEC_PROVINCE);
                // todo 一系列质检的筛选
               qcItems.forEach((k, v) -> {
                   if(allTxt.contains(v)){
                       collector.emit(TOPOLOGY_STREAM_QC_ID, new Values(
                               rowKey, prov, k
                       ));
                   }
               });
            }

        }catch (Exception e) {
            logger.error("quality has error", e);
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(TOPOLOGY_STREAM_QC_ID, new Fields(DEC_ROW_KEY, DEC_PROVINCE, DEC_QC_ITEM));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
