package TranslateFramework;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import javafx.util.Pair;

import java.sql.Time;
import java.util.*;

/**
 * Created by swrrt on 30/9/2015.
 */
public class SPatchMergeBolt extends BaseRichBolt {
    OutputCollector collector;
    int npack;
    Map<Pair<String,Pair<Integer,Pair<Integer,Pair<Integer,Integer>>>>,Pair<Map<Integer,List<Double>>,Integer>> featuretable;
    Set<Pair<String,Pair<Integer,Pair<Integer,Pair<Integer,Integer>>>>> over;
    int zzz = 0, latency;
    public SPatchMergeBolt(int _npack){
        npack = _npack;
    }
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector _collector){
        collector = _collector;
        featuretable = new HashMap<>();
        over = new HashSet<>();
        zzz=0;latency = 20000;
    }
    @Override
    public void execute(Tuple tuple){
        Pair<String,Pair<Integer,Pair<Integer,Pair<Integer,Integer>>>> key = new Pair(tuple.getStringByField("Filename"),new Pair(tuple.getIntegerByField("Pack"),new Pair(tuple.getIntegerByField("Frame"),new Pair(tuple.getIntegerByField("Patch"),tuple.getIntegerByField("Scale")))));
        if(over.contains(key)){
            collector.ack(tuple);return ;
        }
        zzz++;
        if(!featuretable.containsKey(key)){
            featuretable.put(key, new Pair(new TreeMap<>(),zzz));
        }
        featuretable.get(key).getKey().put(tuple.getIntegerByField("sPatch"), (List<Double>) tuple.getValueByField("Feature"));
        System.out.println("SPatch  !!!  "+String.valueOf(featuretable.get(key).getKey().size()));
        if(featuretable.get(key).getKey().size()>=npack||zzz-featuretable.get(key).getValue()>latency){
            over.add(key);
            Iterator<Map.Entry<Integer, List<Double>>> x = featuretable.get(key).getKey().entrySet().iterator();
            List<List<Double>> features = new ArrayList<>();
            for (; x.hasNext(); ) {
                features.add(x.next().getValue());
            }
            featuretable.remove(key);
            //System.out.println("[ SPMatch ]"+tuple.getIntegerByField("Pack")+" "+features);
            collector.emit(tuple,new Values(features,tuple.getStringByField("Filename"), tuple.getIntegerByField("Pack"), tuple.getIntegerByField("Frame"), tuple.getIntegerByField("Patch"), tuple.getIntegerByField("Scale"),0));
        }
        collector.ack(tuple);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("Features","Filename","Pack","Frame","Patch","Scale","sPatch"));
    }
}
