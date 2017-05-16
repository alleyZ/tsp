package com.alleyz.tsp.topo.logocal;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TupleHelpers;
import com.alleyz.tsp.nplir.NLPIRUtil;
import com.alleyz.tsp.topo.constant.TopoConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by alleyz on 2017/5/16.
 * 分词
 */
public class SegmentBolt implements IBasicBolt{
    private static Logger logger = LoggerFactory.getLogger(NewWordBolt.class);
    @Override
    public void prepare(Map stormConf, TopologyContext context) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(TopoConstant.TOPOLOGY_STREAM_SEG_WORD_ID,
                new Fields(TopoConstant.DEC_ROW_KEY, TopoConstant.DEC_SEG_WORD));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(TupleHelpers.isTickTuple(input)) {
            logger.debug("SegmentBolt - Receive one Ticket Tuple " + input.getSourceComponent());
            return;
        }

        if(TopoConstant.TOPOLOGY_STREAM_TXT_ID.equals(input.getSourceStreamId())) {
            String rowKey = input.getStringByField(TopoConstant.DEC_ROW_KEY);
            String allTxt = input.getStringByField(TopoConstant.DEC_ALL_TXT);
            if(allTxt == null || allTxt.length() == 0) return;
            String words = NLPIRUtil.segment(allTxt, false);
            //todo 去除噪声词
            if(words != null && words.length() > 0) {
                collector.emit(TopoConstant.TOPOLOGY_STREAM_SEG_WORD_ID, new Values(
                   rowKey, words
                ));
            }
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
