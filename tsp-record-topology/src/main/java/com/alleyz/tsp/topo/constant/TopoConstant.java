package com.alleyz.tsp.topo.constant;

/**
 * Created by alleyz on 2017/5/16.
 *
 */
public class TopoConstant {

    public final static String TOPOLOGY_MODE = "topology.mode";

    public final static String TOPOLOGY_STREAM_TXT_ID = "txt-spout-stream";
    public final static String TOPOLOGY_STREAM_NEW_WORD_ID = "new-word-stream";
    public final static String TOPOLOGY_STREAM_SEG_WORD_ID = "seg-word-stream";
    public final static String TOPOLOGY_STREAM_HBASE_ID = "hbase-stream";
    public final static String TOPOLOGY_STREAM_QC_ID = "qc-stream";
    public final static String TOPOLOGY_STREAM_STORE_ORI_ID = "store-ori-stream";

    public final static String DEC_BATCH_ID = "batchId";
    public final static String DEC_ROW_KEY = "rowKey";
    public final static String DEC_BASIC_INFO = "basicInfo";
    public final static String DEC_PROVINCE = "province";
    public final static String DEC_DAY = "day";
    public final static String DEC_AGENT_TXT = "agentTxt";
    public final static String DEC_USER_TXT = "userTxt";
    public final static String DEC_ALL_TXT = "allTxt";

    public final static String DEC_NW_WORD = "nwWord";
    public final static String DEC_NW_WORD_NATURE = "nwNature";
    public final static String DEC_NW_WORD_WEIGHT = "nwWeight";
    public final static String DEC_NW_WORD_FREQ = "nwFreq";

    public final static String DEC_SEG_WORD = "segWord";
    public final static String DEC_QC_ITEM = "qcItem";

    public final static String TOPOLOGY_PARALLEL_BOLT_INDEX = "bolt.parallel.index";
    public final static String TOPOLOGY_PARALLEL_BOLT_NW = "bolt.parallel.newWord";
    public final static String TOPOLOGY_PARALLEL_BOLT_SEG = "bolt.parallel.seg";
    public final static String TOPOLOGY_PARALLEL_BOLT_STORE_NW = "bolt.parallel.storeNewWord";
    public final static String TOPOLOGY_PARALLEL_BOLT_HBASE = "bolt.parallel.hbase";
    public final static String TOPOLOGY_PARALLEL_SPOUT = "spout.parallel";
    public final static String TOPOLOGY_PARALLEL_QC = "bolt.parallel.qc";
    public final static String TOPOLOGY_PARALLEL_ORACLE = "bolt.parallel.oracle";


    public final static String PATH_FORMAT_PROV = "{prov}";
    public final static String PATH_FORMAT_MONTH = "{month}";

    public final static String PATH_HDFS_NEW_WORD = "path.hdfs.newWord";
    public final static String PATH_HDFS_SEG = "path.hdfs.senment";
    public final static String PATH_HDFS_ORI = "path.hdfs.origin";

    public final static String HADOOP_USER = "hadoop.user";
    public final static String HADOOP_USER_NAME = "HADOOP_USER_NAME";
}
