package com.alleyz.tsp.topo.logocal;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alleyz.tsp.nplir.NLPIRUtil;
import com.alleyz.tsp.topo.constant.TopoConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.alleyz.tsp.topo.constant.TopoConstant.*;

/**
 * Created by alleyz on 2017/5/16.
 * 新词学习
 */
public class NewWordBolt implements IBasicBolt {
    private static Logger logger = LoggerFactory.getLogger(NewWordBolt.class);
    public static final String NAME = "newWord-bolt";
    @Override
    public void prepare(Map stormConf, TopologyContext context) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(TopoConstant.TOPOLOGY_STREAM_TXT_ID.equals(input.getSourceStreamId())) {
            try {
                String allTxt = input.getStringByField(TopoConstant.DEC_ALL_TXT);
                logger.info("new word ++++++++++++ ");
                String newWords = NLPIRUtil.getNewWords(allTxt, 50, true);
                logger.info("new word ++++++++++++ " + newWords);
                if (newWords != null && newWords.length() > 0) {
                    String[] words = newWords.split("#");
                    for (String word : words) {
                        String[] wordMeta = word.split("/");
                        if (wordMeta.length != 4) continue;
                        collector.emit(TOPOLOGY_STREAM_NEW_WORD_ID, new Values(
                                wordMeta[0], wordMeta[1], wordMeta[2], wordMeta[3]
                        ));
                    }
                }
            }catch (Exception e){
                logger.error("new word has err", e);
            }
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(TOPOLOGY_STREAM_NEW_WORD_ID, new Fields(
            DEC_NW_WORD, DEC_NW_WORD_NATURE, DEC_NW_WORD_WEIGHT, DEC_NW_WORD_FREQ
        ));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
