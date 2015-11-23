package TranslateFramework;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by swrrt on 16/10/2015.
 */
public class TrajShapeFeatureBolt extends BaseRichBolt {
    OutputCollector collector;
    @Override
    public void prepare(Map conf,TopologyContext context, OutputCollector _collector){
        collector = _collector;
    }
    @Override
    public void execute(Tuple tuple){
        List <Double> feature = new ArrayList<>(), xs = (List<Double>)tuple.getValueByField("Xs"),ys = (List<Double>)tuple.getValueByField("Ys");
        double s=0;
        for(int i=0;i<xs.size();i++){
            double x = xs.get(i),y = ys.get(i);
            feature.add(x);
            feature.add(y);
            s+=x*x+y*y;
        }
        s = Math.sqrt(s);
        for(int i=0;i<feature.size();i++){
            feature.set(i,feature.get(i)/s);
        }
        collector.emit(new Values(feature,"Shape",tuple.getStringByField("Filename"),tuple.getIntegerByField("Pack"),tuple.getIntegerByField("Frame"),tuple.getIntegerByField("Patch"),tuple.getIntegerByField("Scale"),tuple.getIntegerByField("sPatch")));
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("Feature","F_type","Filename","Pack","Frame","Patch","Scale","sPatch"));
    }
}
