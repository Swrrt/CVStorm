package TranslateFramework;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by swrrt on 30/9/2015.
 */
public class FeatureMergeBolt_Latent extends BaseRichBolt {
    OutputCollector collector;
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector _collector){
        collector = _collector;
    }
    @Override
    public void execute(Tuple tuple){
        collector.ack(tuple);
        collector.emit(tuple,new Values(tuple.getValue(0),tuple.getValue(1),tuple.getValue(2),tuple.getValue(3),tuple.getValue(4),tuple.getValue(5),tuple.getValue(6),tuple.getValue(7)));
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("Feature","F_type","Filename","Pack","Frame","Patch","Scale","sPatch"));
    }
}
