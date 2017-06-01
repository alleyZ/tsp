package com.alleyz.tsp.topo.batch;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TupleHelpers;
import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.IBatchSpout;
import com.alleyz.tsp.config.ConfigUtils;
import com.alleyz.tsp.kafka.consumer.SimpleConsumer;
import com.alleyz.tsp.topo.logic.TxtSpout;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static com.alleyz.tsp.constant.Constant.*;
import static com.alleyz.tsp.constant.Constant.DELIMITER_FIELDS;
import static com.alleyz.tsp.topo.constant.TopoConstant.*;
import static com.alleyz.tsp.topo.constant.TopoConstant.DEC_ALL_TXT;

/**
 * Created by alleyz on 2017/6/1.
 */
public class TxtBatchSpout implements IBatchSpout {
    public static final String NAME = "txt-batch-spout";
    private static final Logger logger = LoggerFactory.getLogger(TxtBatchSpout.class);
    private SimpleConsumer consumer;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        Properties props = ConfigUtils.getProp("/consumer.properties");
        Long interval = ConfigUtils.getLongVal("poll.interval.mills", 1000L);
        String group = (String)stormConf.get(MSG_GROUP);
        this.consumer = new SimpleConsumer(TXT_MSG_TOPIC, group == null ? TXT_MSG_GROUP_TOPOLOGY : group,
                props, interval);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(TupleHelpers.isTickTuple(input)) return;
        BatchId batchId = (BatchId) input.getValue(0);
        logger.debug("txt-batch-spout batch is " + batchId.getId());
        this.consumer.pollAndProcessMsg(crs -> {
            Iterator<ConsumerRecord<String, String>> iterator = crs.iterator();
            while (iterator.hasNext()){
                ConsumerRecord<String, String> record = iterator.next();
                String rowKey = record.key();
                String value = record.value();
                String[] values = value.split(DELIMITER_BLOCK);
                String basicInfo = values[0];
                String[] txt = values[1].split(DELIMITER_FIELDS);
                try {
                    collector.emit(
                            TOPOLOGY_STREAM_TXT_ID,
                            new Values(
                                    batchId,
                                    rowKey,
                                    rowKey.substring(10, 12),
                                    StringUtils.reverse(rowKey.substring(0, 10))
                                            .substring(0, 8),
                                    basicInfo,
                                    txt[0],
                                    txt[1],
                                    txt[2]
                            ));

                }catch (Exception e){
                    logger.error("txt-spout has err", e);
                }
            }
            return true;
        });
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(
                TOPOLOGY_STREAM_TXT_ID,
                new Fields(DEC_BATCH_ID, DEC_ROW_KEY, DEC_PROVINCE, DEC_DAY,
                        DEC_BASIC_INFO, DEC_USER_TXT, DEC_AGENT_TXT, DEC_ALL_TXT)
        );
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public byte[] commit(BatchId id) throws FailedException {
        logger.debug("txt-batch-spout commit " + id.getId());
        return null;
    }

    @Override
    public void revert(BatchId id, byte[] commitResult) {
        logger.debug("txt-batch-spout revert " + id.getId());
    }
}
