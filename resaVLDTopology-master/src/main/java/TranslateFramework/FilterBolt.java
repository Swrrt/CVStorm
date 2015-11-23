package TranslateFramework;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import tool.Serializable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by swrrt on 27/10/2015.
 */
public class FilterBolt extends BaseRichBolt {
    OutputCollector  collector;
    List<String>name, value;
    int n;
    public FilterBolt(List<String> ex, List<String> _value){
        name = new ArrayList<>();
        name.addAll(ex);
        value = new ArrayList<>();
        value.addAll(_value);
        n = name.size();
    }
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector _collector){
        collector = _collector;
    }
    @Override
    public void execute(Tuple tuple){
        collector.ack(tuple);
        for(int i=0;i<n;i++){
            if(!tuple.contains(name.get(i))||tuple.getValueByField(name.get(i))==null||String.valueOf(tuple.getValueByField(name.get(i))).compareTo(value.get(i))!=0){
                return;
            }
        }
        Serializable.Mat img=null;
        List<Serializable.Mat> imgs=null;
        List<List<Double>> features=null;
        List<Double> feature=null;
        String F_type=null,filename=null;
        Integer pack=null,frame=null,patch=null,scale=null,spatch=null;
        List<Double[][]> OFxs=null,OFys=null;
        Double[][] OFx=null,OFy=null;
        List<Double>Xs=null,Ys=null;
        if(tuple.contains("Image")){
            img = (Serializable.Mat)tuple.getValueByField("Image");
        }
        if(tuple.contains("Images")){
            imgs = (List<Serializable.Mat>)tuple.getValueByField("Images");
        }
        if(tuple.contains("Feature")){
            feature = (List<Double>)tuple.getValueByField("Feature");
        }
        if(tuple.contains("Features")){
            features = (List<List<Double>>)tuple.getValueByField("Features");
        }
        if(tuple.contains("F_type")){
            F_type = tuple.getStringByField("F_type");
        }
        if(tuple.contains("Filename")){
            filename = tuple.getStringByField("Filename");
        }
        if(tuple.contains("Pack")){
            pack = tuple.getIntegerByField("Pack");
        }
        if(tuple.contains("Frame")){
            frame = tuple.getIntegerByField("Frame");
        }
        if(tuple.contains("Patch")){
            patch = tuple.getIntegerByField("Patch");
        }
        if(tuple.contains("Scale")){
            scale = tuple.getIntegerByField("Scale");
        }
        if(tuple.contains("sPatch")){
            spatch = tuple.getIntegerByField("sPatch");
        }
        if(tuple.contains("OFxs")){
            OFxs = (List<Double[][]>)tuple.getValueByField("OFxs");
        }
        if(tuple.contains("OFys")){
            OFys = (List<Double[][]>)tuple.getValueByField("OFys");
        }
        if(tuple.contains("OFx")){
            OFx = (Double[][])tuple.getValueByField("OFx");
        }
        if(tuple.contains("OFy")){
            OFy = (Double[][])tuple.getValueByField("OFy");
        }
        if(tuple.contains("Xs")){
            Xs = (List<Double>)tuple.getValueByField("Xs");
        }
        if(tuple.contains("Ys")){
            Ys = (List<Double>)tuple.getValueByField("Ys");
        }
        collector.emit(tuple,new Values(imgs,img,feature,features,F_type,filename,pack,frame,patch,scale,spatch,OFx,OFy,OFxs,OFys,Xs,Ys));
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("Images","Image","Feature","Features","F_type","Filename","Pack","Frame","Patch","Scale","sPatch","OFx","OFy","OFxs","OFys","Xs","Ys"));
    }
}
