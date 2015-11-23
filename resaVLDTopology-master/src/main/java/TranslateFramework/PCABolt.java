package TranslateFramework;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.bytedeco.javacpp.opencv_core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by swrrt on 8/9/2015.
 */
public class PCABolt extends BaseRichBolt {
    opencv_core.Mat PCAmat;
    OutputCollector collector;
    @Override
    public void prepare(Map Conf,TopologyContext context, OutputCollector _collector){
        collector = _collector;
        PCAmat = new opencv_core.Mat();

    }
    @Override
    public void execute(Tuple tuple){
        List<List<Double>> feature = (List<List<Double>>)tuple.getValueByField("Feature");
        List<List<Double>> encodedFeature = new ArrayList<>();
        for(int i=0;i<feature.size();i++) {
            List<Double> a = new ArrayList<Double>();
            for(int k=0;k < PCAmat.cols();k++){
                double s=0;
                for(int j=0;j<feature.get(0).size();j++){
                    s+=PCAmat.ptr(j,k).get()*feature.get(i).get(j);
                }
                a.add(s);
            }
            encodedFeature.add(a);
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){

    }
}