package com.alleyz.tsp.topo.logic;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alleyz.tsp.config.ConfigUtils;
import com.alleyz.tsp.constant.Constant;
import com.alleyz.tsp.topo.constant.TopoConstant;
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
import java.util.Map;

import static com.alleyz.tsp.constant.Constant.*;
import static com.alleyz.tsp.topo.constant.TopoConstant.*;

/**
 * Created by alleyz on 2017/5/16.
 *
 */
public class StoreHBaseBolt implements IBasicBolt{
    private static Logger logger = LoggerFactory.getLogger(StoreHBaseBolt.class);
    public static final String NAME = "hbase-store";
    private Connection connection;
    private static byte[] infoFamily = Bytes.toBytes(CUST_INFO_H_FAMILY),
        txtFamily = Bytes.toBytes(CUST_TXT_H_FAMILY),
        infoQua = Bytes.toBytes(CUST_INFO_H_QUA),
        userTxtQua = Bytes.toBytes(CUST_USER_TXT_H_QUE),
        agentTxtQua = Bytes.toBytes(CUST_AGENT_TXT_H_QUA),
        allTxtQua = Bytes.toBytes(CUST_ALL_TXT_H_QUA);
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
        if(TopoConstant.TOPOLOGY_STREAM_TXT_ID.equals(input.getSourceStreamId())) {
            String rowKey = input.getStringByField(DEC_ROW_KEY);
            String basicInfo = input.getStringByField(DEC_BASIC_INFO);
            String agentTxt = input.getStringByField(DEC_AGENT_TXT);
            String userTxt = input.getStringByField(DEC_USER_TXT);
            String prov = input.getStringByField(DEC_PROVINCE);
            String allTxt = input.getStringByField(TopoConstant.DEC_ALL_TXT);
            Put put = transfer2Put(rowKey, basicInfo, allTxt, agentTxt, userTxt);
            // 此table非线程安全 且使用完毕须关闭
            try (Table table = connection.getTable(TableName.valueOf(Constant.CUST_INFO_H_TABLE))){
                table.put(put);
                collector.emit(TOPOLOGY_STREAM_HBASE_ID, new Values(
                        rowKey, prov, basicInfo, agentTxt, userTxt, allTxt
                ));
            }catch (IOException e) {
                e.printStackTrace();
                logger.error("hbase--> execute has error", e);
            }
        }
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
        try {
            this.connection.close();
        }catch (IOException e) {
            logger.error("close hbase connection has err", e);
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(TopoConstant.TOPOLOGY_STREAM_HBASE_ID, new Fields(
                DEC_ROW_KEY, DEC_PROVINCE, DEC_BASIC_INFO, DEC_AGENT_TXT, DEC_USER_TXT, DEC_ALL_TXT
        ));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
