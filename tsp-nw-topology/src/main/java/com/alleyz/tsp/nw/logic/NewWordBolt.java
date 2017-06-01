package com.alleyz.tsp.nw.logic;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alleyz.tsp.nlpir.NLPIRUtil;

import java.util.Map;

import static com.alleyz.tsp.nw.constant.Constant.*;

/**
 * Created by alleyz on 2017/5/31.
 * 7252
 */
public class NewWordBolt implements IBasicBolt{
    public final static String NAME = "new-word-bolt";
    @Override
    public void prepare(Map stormConf, TopologyContext context) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(TOPOLOGY_STREAM_NW_SPOUT_ID.equals(input.getSourceStreamId())) {
            String allTxt = input.getStringByField(DEC_TXT);
            String nws = NLPIRUtil.getNewWords(allTxt, 50, true);
            if(nws == null || "".equals(nws)) return;
            String word = nws.substring(0, nws.indexOf("/"));
            String freq = nws.substring(nws.indexOf("/") + 1);
            collector.emit(TOPOLOGY_STREAM_NW_ID, new Values(word, freq));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(TOPOLOGY_STREAM_NW_ID, new Fields(DEC_WORD, DEC_FREQ));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
