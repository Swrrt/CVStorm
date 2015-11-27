package TranslateFramework;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import javafx.util.Pair;


import java.util.*;

/**
 * Created by swrrt on 29/9/2015.
 */
public class FeatureMergeBolt extends BaseRichBolt {
    OutputCollector collector;
    Map<Pair<String,Pair<Integer,Pair<Integer,Pair<Integer,Integer>>>>,Map<Integer,List<Double>>> feature;
    Set<Pair<String,Pair<Integer,Pair<Integer,Pair<Integer,Integer>>>>> over;
    String name;
    int npack;
    int pps;
    Map<String,Integer> ppp;
    public FeatureMergeBolt(int _npack){
        npack = _npack;
    }
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector _collector) {
        collector = _collector;
        name = context.getThisComponentId();
        feature = new HashMap<>();
        over = new HashSet<>();
        ppp = new HashMap<>();
        pps = 0;
    }
    public int TypeToInt(String x){
        /*if(x.contains("HOG"))return 0;
        if(x.contains("HOF"))return 1;
        if(x.contains("MBH"))return 2;
        if(x.contains("Shape"))return 3;
        return 4;*/
        int hash=0,mo = 100007,p1 = 113;
        for(int i=0;i<x.length();i++){
            hash*=p1;
            hash+=x.charAt(i);
            if(hash>=mo)hash%=mo;
        }
        hash*=10;
        hash+=x.length();
        /*if(ppp.containsKey(x))return ppp.get(x);
        else {
            pps++;
            ppp.put(x,pps);
            return pps;
        }*/
	return hash;
    }
    @Override
    public void execute(Tuple tuple) {
        collector.ack(tuple);
        Pair<String,Pair<Integer,Pair<Integer,Pair<Integer,Integer>>>> key = new Pair(tuple.getStringByField("Filename"),new Pair(tuple.getIntegerByField("Pack"),new Pair(tuple.getIntegerByField("Patch"),new Pair(tuple.getIntegerByField("Scale"),tuple.getIntegerByField("sPatch")))));
        if(over.contains(key))return ;
        if(!feature.containsKey(key)){
            feature.put(key,new TreeMap());
        }

        feature.get(key).put((TypeToInt(tuple.getStringByField("F_type")) * 100 + tuple.getIntegerByField("Frame")), (List<Double>) tuple.getValueByField("Feature"));
        System.out.printf("FeatureMerge %d\n",feature.get(key).size());
        if(feature.get(key).size()>=npack){
            over.add(key);
            List<Double> featur = new ArrayList<>();
            Iterator<Map.Entry<Integer,List<Double>>> x = feature.get(key).entrySet().iterator();
            for(;x.hasNext();){
                Map.Entry<Integer,List<Double>> y = x.next();
                for(int i=0;i<y.getValue().size();i++){
                    featur.add(y.getValue().get(i));
                }
            }
            //System.out.println("[ Merged ] " + String.valueOf(featur.size()) + " " + String.valueOf(feature.get(key).size()));
            feature.remove(key);
            collector.emit(tuple,new Values(featur,name,tuple.getStringByField("Filename"),tuple.getIntegerByField("Pack"),0,tuple.getIntegerByField("Patch"),tuple.getIntegerByField("Scale"),tuple.getIntegerByField("sPatch")));
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("Feature","F_type","Filename","Pack","Frame","Patch","Scale","sPatch"));
    }
}
