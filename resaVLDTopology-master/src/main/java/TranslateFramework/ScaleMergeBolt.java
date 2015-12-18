package TranslateFramework;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import javafx.util.Pair;
import org.bytedeco.javacpp.opencv_core;

import java.util.*;

/**
 * Created by swrrt on 22/10/2015.
 */
public class ScaleMergeBolt extends BaseRichBolt {
    OutputCollector collector;
    int n;
    String name;
    Map<Pair<String,Pair<Integer,Pair<Integer,Pair<Integer,Integer>>>>,Map<Integer,List<Double>>> buffer;
    Set<Pair<String,Pair<Integer,Pair<Integer,Pair<Integer,Integer>>>>> over;
    public ScaleMergeBolt(int x){
        n=x;
    }
    @Override
    public void prepare(Map conf,TopologyContext context, OutputCollector _collector)
    {
        collector = _collector;
        buffer = new HashMap<>();
        over = new HashSet<>();
        name = context.getThisComponentId();
    }
    @Override
    public void execute(Tuple tuple){
        opencv_core.IplImage fkImpage = new opencv_core.IplImage();
        collector.ack(tuple);
        Pair<String,Pair<Integer,Pair<Integer,Pair<Integer,Integer>>>> key = new Pair(tuple.getStringByField("Filename"),new Pair(tuple.getIntegerByField("Pack"),new Pair(tuple.getIntegerByField("Frame"),new Pair(tuple.getIntegerByField("Patch"),tuple.getIntegerByField("sPatch")))));
        if(!over.contains(key)){
        if(!buffer.containsKey(key)){
            buffer.put(key,new TreeMap<>());
        }
        buffer.get(key).put(tuple.getIntegerByField("Scale"), (List<Double>) tuple.getValueByField("Feature"));
        if(buffer.get(key).size()>=n){
            List<Double> feature = new ArrayList<>();
            Iterator<Map.Entry<Integer,List<Double>>> p = buffer.get(key).entrySet().iterator();
            for(;p.hasNext();){
                Map.Entry<Integer,List<Double>> x = p.next();
                feature.addAll(x.getValue());
            }
            collector.emit(tuple,new Values(feature,"SMERGE_"+name,tuple.getStringByField("Filename"),tuple.getIntegerByField("Pack"),tuple.getIntegerByField("Frame"),tuple.getIntegerByField("Patch"),0,tuple.getIntegerByField("sPatch")));
            buffer.remove(key);
            over.add(key);
        }
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("Feature","F_type","Filename","Pack","Frame","Patch","Scale","sPatch"));
    }
}
