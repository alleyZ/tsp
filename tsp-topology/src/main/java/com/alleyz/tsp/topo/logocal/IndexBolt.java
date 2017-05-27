package com.alleyz.tsp.topo.logocal;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alleyz.tsp.config.ConfigUtils;
import com.alleyz.tsp.constant.ConstUtils;
import com.alleyz.tsp.constant.Constant;
import com.alleyz.tsp.topo.constant.TopoConstant;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.alleyz.tsp.constant.Constant.*;

/**
 * Created by alleyz on 2017/5/16.
 *
 */
public class IndexBolt implements IBasicBolt{
    private static Logger logger = LoggerFactory.getLogger(IndexBolt.class);
    public static final String NAME = "index-bolt";
    private Map<String, Integer> mapping;
    private SolrClient client;
    private Integer sheetNoIdx;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        mapping = Constant.getHBaseMapping();
        mapping.remove(recordName);
        this.sheetNoIdx = mapping.get(sheetNo);
        mapping.remove(sheetNo);
        mapping.remove(recordFormat);
        mapping.remove(recordEncodeRate);
        mapping.remove(recordSampRate);
        mapping.remove(custinfoId);
        initSolrClient();
    }

    private void initSolrClient() {
        CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder()
                .withZkHost(ConfigUtils.getStrVal("solr.zk"))
                .build();
        cloudSolrClient.setZkConnectTimeout(100000);
        this.client = cloudSolrClient;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(TopoConstant.TOPOLOGY_STREAM_HBASE_ID.equals(input.getSourceStreamId())) {
            try {
                String rowKey = input.getStringByField(TopoConstant.DEC_ROW_KEY);
                String basicInfo = input.getStringByField(TopoConstant.DEC_BASIC_INFO);
                String agentTxt = input.getStringByField(TopoConstant.DEC_AGENT_TXT);
                String userTxt = input.getStringByField(TopoConstant.DEC_USER_TXT);
                String allTxt = input.getStringByField(TopoConstant.DEC_ALL_TXT);
                String prov = input.getStringByField(TopoConstant.DEC_PROVINCE);
                String[] bis = basicInfo.split(Constant.DELIMITER_FIELDS);
                SolrInputDocument doc = transferDoc(rowKey, bis, userTxt, agentTxt, allTxt);
                commit(prov, doc);
            }catch (SolrServerException e) {
                e.printStackTrace();
                logger.error("solr has error", e);
            }catch (IOException e) {
                logger.error("solr has error", e);
                e.printStackTrace();
                initSolrClient();
            }
        }
    }

    private void commit(String prov, SolrInputDocument doc) throws SolrServerException, IOException{
        this.client.add(ConfigUtils.getStrVal("solr.prefix") + prov, doc,
                ConfigUtils.getIntVal("solr.waitCommitMills", 5000));
    }

    private SolrInputDocument transferDoc(String rowKey, String[] bis, String userTxt, String agentTxt, String allTxt) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.setField(id, rowKey);
        doc.setField(allContent, allTxt);
        doc.setField(agentContent, agentTxt);
        doc.setField(userContent, userTxt);

        String sheet ;
        if(sheetNoIdx == null){
            sheet = null;
        }else {
            sheet = bis[sheetNoIdx];
        }
        doc.setField(hasSheet, sheet == null || "".equals(sheet) || "null".equals(sheet) ? "0" : "1");

        long duration =  Long.parseLong(bis[mapping.get(recordLength)]);
        long silence = Long.parseLong(bis[mapping.get(silenceLength)]);
        doc.setField(recordLengthRange, ConstUtils.getRecoinfoLengthRangeCode(duration));
        doc.setField(silenceLengthRange, ConstUtils.getSilenceRangeCode(duration, silence));

        mapping.forEach((field, idx) -> doc.setField(field, bis[idx]));

        return doc;
    }
    @Override
    public void cleanup() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
