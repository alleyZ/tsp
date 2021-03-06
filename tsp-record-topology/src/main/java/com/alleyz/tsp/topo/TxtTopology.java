package com.alleyz.tsp.topo;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import com.alleyz.tsp.config.ConfigUtils;
import com.alleyz.tsp.topo.constant.TopoConstant;
import com.alleyz.tsp.topo.logic.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.alleyz.tsp.topo.constant.TopoConstant.*;
import static com.alleyz.tsp.topo.logic.TxtSpout.NAME;

/**
 * Created by alleyz on 2017/5/16.
 *
 */
public class TxtTopology {
    public final static String TOPOLOGY_NAME = "record-topology";
    private static Logger logger = LoggerFactory.getLogger(TxtTopology.class);

    public static void drawTopology(TopologyBuilder topology, Map<String, Object> conf) {
        // 原始数据
        SpoutDeclarer txtSpout = topology.setSpout(NAME, new TxtSpout(),
                ConfigUtils.getIntVal(conf, TOPOLOGY_PARALLEL_SPOUT, 1));
        // 分词
        BoltDeclarer segBolt = topology.setBolt(SegmentBolt.NAME, new SegmentBolt(),
                ConfigUtils.getIntVal(conf, TOPOLOGY_PARALLEL_BOLT_SEG, 1));
        // 索引
        BoltDeclarer idxBolt = topology.setBolt(IndexBolt.NAME, new IndexBolt(),
                ConfigUtils.getIntVal(conf, TOPOLOGY_PARALLEL_BOLT_INDEX, 1));
        // 存储hbase
        BoltDeclarer hbaseBolt = topology.setBolt(StoreHBaseBolt.NAME, new StoreHBaseBolt(),
                ConfigUtils.getIntVal(conf, TOPOLOGY_PARALLEL_BOLT_HBASE, 1));

//        // 存储原始文本到hdfs中， 由于hdfs文件不支持并发写的操作，所以并行为1
//        BoltDeclarer storeOriBolt = topology.setBolt(StoreOriginBolt.NAME, new StoreOriginBolt(), 1);
//
//        // 存储分词结果到kafka
        BoltDeclarer storeSegBolt = topology.setBolt(SegmentProduceBolt.NAME, new SegmentProduceBolt(), 1);

        // 质检
        BoltDeclarer qcBolt = topology.setBolt(QualityBolt.NAME, new QualityBolt(),
                ConfigUtils.getIntVal(TOPOLOGY_PARALLEL_QC, 1));
        // oracle存储
        BoltDeclarer oracleBolt = topology.setBolt(StoreOracleBolt.NAME, new StoreOracleBolt(),
                ConfigUtils.getIntVal(TOPOLOGY_PARALLEL_ORACLE, 1));

        // 分词 接入原始数据，输出分词结果数据流
        segBolt.allGrouping(TxtSpout.NAME, TopoConstant.TOPOLOGY_STREAM_TXT_ID);
        // hbase存储 接入原始数据 输出hbase数据流
        hbaseBolt.allGrouping(TxtSpout.NAME, TopoConstant.TOPOLOGY_STREAM_TXT_ID);
        // 索引 接入hbase数据流 无输出
        idxBolt.allGrouping(StoreHBaseBolt.NAME, TopoConstant.TOPOLOGY_STREAM_HBASE_ID);
//        // 存储原始文件 输入：原始文本 无输出
//        storeOriBolt.allGrouping(TxtSpout.NAME, TopoConstant.TOPOLOGY_STREAM_TXT_ID);
//        // 存储分词文本 输入 分词结果， 无输出
        storeSegBolt.allGrouping(SegmentBolt.NAME, TopoConstant.TOPOLOGY_STREAM_SEG_WORD_ID);
        // 质检 输入：hbase数据流, 输出：质检结果
        qcBolt.allGrouping(StoreHBaseBolt.NAME, TopoConstant.TOPOLOGY_STREAM_HBASE_ID);
        // 存储Oracle 输入：质检数据流  输出：无
        oracleBolt.allGrouping(QualityBolt.NAME, TopoConstant.TOPOLOGY_STREAM_QC_ID);

    }
}

