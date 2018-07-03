package hank.test.storm.bolt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;


import java.io.IOException;
import java.util.Map;

public class MyHBaseBolt extends BaseBasicBolt{
    private Connection connection;
    private Table table;

    public void prepare(Map stormConf, TopologyContext context) {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","ambari");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        try {
            connection = ConnectionFactory.createConnection(config);
//            table=new HTable(config,"wordcount");
            //示例都是对同一个table进行操作，因此直接将Table对象的创建放在了prepare，在bolt执行过程中可以直接重用。
            table = connection.getTable(TableName.valueOf("wordcount"));
        } catch (IOException e) {
            //do something to handle exception
        }
    }
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //从tuple中获取单词
        String word = tuple.getStringByField("word");
        //从tuple中获取计数，这里转换为String只是为了示例运行后存入hbase的计数值能够直观显示。
        String count = tuple.getStringByField("count");
        try {

            System.out.println("进入--------hbase.....................................................................................................");
            //以各个单词作为row key
            Put put = new Put(Bytes.toBytes(word));
            //将单词的计数写入cf:counts列
                put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("count"), Bytes.toBytes(count));
            System.out.println("开始put.....................................................................................................");

            table.put(put);
            System.out.println("put成功.....................................................................................................");

        } catch (IOException e) {
            //do something to handle exception
            System.out.println("错误信息："+e+"-------------------------------------------------------------------");
            System.out.println("put--------出错.....................................................................................................");
        }
    }

    public void cleanup() {
        //关闭table
        try {
            if (table != null) table.close();
        } catch (Exception e) {
            //do something to handle exception
        } finally {
            //在finally中关闭connection
            System.out.println("关闭hbase.....................................................................................................");
            try {
                connection.close();
            } catch (IOException e) {
                //do something to handle exception
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //示例中本bolt不向外发射数据，所以没有再做声明
    }
}
