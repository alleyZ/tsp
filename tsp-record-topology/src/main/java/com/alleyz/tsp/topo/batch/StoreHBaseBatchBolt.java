package com.alleyz.tsp.topo.batch;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TupleHelpers;
import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.ICommitter;
import com.alleyz.tsp.config.ConfigUtils;
import com.alleyz.tsp.constant.Constant;
import com.alleyz.tsp.topo.constant.TopoConstant;
import com.alleyz.tsp.topo.logic.StoreHBaseBolt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import static com.alleyz.tsp.constant.Constant.*;
import static com.alleyz.tsp.constant.Constant.CUST_AGENT_TXT_H_QUA;
import static com.alleyz.tsp.constant.Constant.CUST_ALL_TXT_H_QUA;
import static com.alleyz.tsp.topo.constant.TopoConstant.*;
import static com.alleyz.tsp.topo.constant.TopoConstant.DEC_PROVINCE;

/**
 * Created by alleyz on 2017/6/1.
 */
public class StoreHBaseBatchBolt implements IBasicBolt, ICommitter {
    private static Logger logger = LoggerFactory.getLogger(StoreHBaseBolt.class);
    public static final String NAME = "hbase-batch-store";
    private Connection connection;
    private static byte[] infoFamily = Bytes.toBytes(CUST_INFO_H_FAMILY),
            txtFamily = Bytes.toBytes(CUST_TXT_H_FAMILY),
            infoQua = Bytes.toBytes(CUST_INFO_H_QUA),
            userTxtQua = Bytes.toBytes(CUST_USER_TXT_H_QUE),
            agentTxtQua = Bytes.toBytes(CUST_AGENT_TXT_H_QUA),
            allTxtQua = Bytes.toBytes(CUST_ALL_TXT_H_QUA);
    private final ConcurrentMap<Long, Queue<Put>> dataMaps = new ConcurrentHashMap<>();

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set(Constant.HBASE_ZK_QUORUM, ConfigUtils.getStrVal(Constant.HBASE_ZK_QUORUM));
            conf.set(Constant.HBASE_ZK_QUORUM_PORT, ConfigUtils.getStrVal(Constant.HBASE_ZK_QUORUM_PORT));
            this.connection = ConnectionFactory.createConnection(conf);
        }catch (IOException e) {
            logger.error("init hbase connection has error", e);
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
//        if("streamId".equals(input.getSourceStreamId())) {
        if(TupleHelpers.isTickTuple(input)) return;
        BatchId batchId = (BatchId) input.getValue(0);
        String rowKey = input.getStringByField(DEC_ROW_KEY);
        String basicInfo = input.getStringByField(DEC_BASIC_INFO);
        String agentTxt = input.getStringByField(DEC_AGENT_TXT);
        String userTxt = input.getStringByField(DEC_USER_TXT);
        String prov = input.getStringByField(DEC_PROVINCE);
        String allTxt = input.getStringByField(TopoConstant.DEC_ALL_TXT);
        Put put = transfer2Put(rowKey, basicInfo, allTxt, agentTxt, userTxt);
        Queue<Put> queue =  dataMaps.computeIfAbsent(batchId.getId(), (k) -> new ConcurrentLinkedQueue<>());
        queue.add(put);
        collector.emit(new Values(batchId, rowKey, prov, basicInfo, agentTxt, userTxt, allTxt));
    }
    private static Put transfer2Put(String rowKey, String info, String allTxt, String agentTxt, String userTxt) {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(infoFamily, infoQua, Bytes.toBytes(info));
        put.addColumn(txtFamily, allTxtQua, Bytes.toBytes(allTxt));
        put.addColumn(txtFamily, agentTxtQua, Bytes.toBytes(agentTxt));
        put.addColumn(txtFamily, userTxtQua, Bytes.toBytes(userTxt));
        return put;
    }
    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(DEC_BATCH_ID, DEC_ROW_KEY, DEC_PROVINCE, DEC_DAY,
                DEC_BASIC_INFO, DEC_USER_TXT, DEC_AGENT_TXT, DEC_ALL_TXT));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public byte[] commit(BatchId id) throws FailedException {
        Queue<Put> queue = dataMaps.get(id.getId());
        if(queue != null && queue.size() > 0) {
            try (Table table = connection.getTable(TableName.valueOf(Constant.CUST_INFO_H_TABLE))) {
                table.put( new ArrayList<>(queue));
            }catch (IOException e){
                logger.error("save hbase, batch id is " + id.getId(), e);
            }
        }
        return "success".getBytes();
    }

    @Override
    public void revert(BatchId id, byte[] commitResult) {
        System.out.println(id + "  revert =================== " + new String(commitResult));
    }
}
