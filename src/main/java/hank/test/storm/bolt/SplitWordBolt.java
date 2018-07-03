package hank.test.storm.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitWordBolt extends BaseBasicBolt {
    public void execute(Tuple input, BasicOutputCollector collector) {
        String value = input.getString(0);
        String[] split = value.split(" ");
        System.out.println("WordSplitBolt  RECEIVE:  " + value);
        for (String s : split) {
            collector.emit(new Values(s));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
