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
public class SPatchMergeBolt_Latent extends BaseRichBolt {
    OutputCollector collector;
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector _collector){
        collector = _collector;
    }
    @Override
    public void execute(Tuple tuple){
        collector.ack(tuple);
        collector.emit(tuple,new Values(tuple.getValueByField("Feature"),tuple.getValueByField("F_type"),tuple.getValueByField("Filename"),tuple.getIntegerByField("Pack"),tuple.getValueByField("Frame"),tuple.getValueByField("Patch"),tuple.getValueByField("Scale"),tuple.getValueByField("sPatch")));
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("Feature","F_type","Filename","Pack","Frame","Patch","Scale","sPatch"));
    }
}
