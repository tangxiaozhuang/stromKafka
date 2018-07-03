package hank.test.storm.topology;

import com.google.common.collect.Maps;
import hank.test.storm.bolt.MyHBaseBolt;
import hank.test.storm.bolt.SplitWordBolt;
import hank.test.storm.bolt.WordCountBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.hbase.security.HBaseSecurityUtil;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

public class WordCountTopology {
    private static final String zkHost = "ambari:2181/kafka"; //zookeeper host
    private static final String testTopic = "test"; //测试主题
    private static final String zkRoot = "/kafka"; //zookeeper 根节点
    private static final String ID = "word";
    private static final String TOPOLOGY_NAME = "storm-hbase-topology-1";

    public static void main(String[] args){
        TopologyBuilder builder=new TopologyBuilder();
        BrokerHosts brokerHosts=new ZkHosts(zkHost,"/brokers");
        SpoutConfig spoutConfig=new SpoutConfig(brokerHosts,testTopic,zkRoot,ID);
        spoutConfig.socketTimeoutMs = 60 * 1000 ;
        spoutConfig.scheme=new SchemeAsMultiScheme(new StringScheme());
//        spoutConfig.forceFromStart=false;
        //设置hbasebolt
        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField("word")
                .withColumnFields(new Fields("count"))
                .withColumnFamily("result");
        Map<String, Object> map = Maps.newTreeMap();
        map.put("hbase.rootdir", "hdfs://ambari:9000/user/hbase");
        map.put("hbase.zookeeper.quorum", "ambari:2181");
        HBaseBolt hbaseBolt = new HBaseBolt("wordcount", mapper).withConfigKey("hbase");

        builder.setSpout("kafkaSpout",new KafkaSpout(spoutConfig),1);
        builder.setBolt("splitWordBolt", new SplitWordBolt(), 1).setNumTasks(1)
                .shuffleGrouping("kafkaSpout");

        builder.setBolt("countBolt", new WordCountBolt(), 1)
                .fieldsGrouping("splitWordBolt", new Fields("word"));
        builder.setBolt("HbaseBolt", new MyHBaseBolt(), 1).globalGrouping("countBolt");

/*        builder.setBolt("HbaseBolt", hbaseBolt, 1)
                .addConfiguration("hbase", new HashMap<String, Object>())
                .shuffleGrouping("countBolt");*/
        Config conf = new Config();
        conf.setDebug(true);
/*        conf.put("hbase", map);*/
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
/*        Utils.sleep(100000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();*/
    }
}
