package com.alleyz.tsp.topo.logocal;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alleyz.tsp.config.ConfigUtil;
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

import java.io.IOException;
import java.util.Map;

import static com.alleyz.tsp.constant.Constant.*;

/**
 * Created by alleyz on 2017/5/16.
 *
 */
public class StoreHBaseBolt implements IBasicBolt{

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
            conf.set(Constant.HBASE_ZK_QUORUM, ConfigUtil.getStrVal(Constant.HBASE_ZK_QUORUM));
            conf.set(Constant.HBASE_ZK_QUORUM_PORT, ConfigUtil.getStrVal(Constant.HBASE_ZK_QUORUM_PORT));
            this.connection = ConnectionFactory.createConnection(conf);
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(TopoConstant.TOPOLOGY_STREAM_TXT_ID.equals(input.getSourceStreamId())) {
            String rowKey = input.getStringByField(TopoConstant.DEC_ROW_KEY);
            String basicInfo = input.getStringByField(TopoConstant.DEC_BASIC_INFO);
            String agentTxt = input.getStringByField(TopoConstant.DEC_AGENT_TXT);
            String userTxt = input.getStringByField(TopoConstant.DEC_USER_TXT);
            String allTxt = input.getStringByField(TopoConstant.DEC_ALL_TXT);
            Put put = transfer2Put(rowKey, basicInfo, allTxt, agentTxt, userTxt);
            // 此table非线程安全 且使用完毕须关闭
            try (Table table = connection.getTable(TableName.valueOf(Constant.CUST_INFO_H_TABLE))){
                table.put(put);
            }catch (IOException e) {
                e.printStackTrace();
                collector.reportError(e);
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

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
