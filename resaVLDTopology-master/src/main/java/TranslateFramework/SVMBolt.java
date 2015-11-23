package TranslateFramework;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Values;
import libsvm.*;
/**
 * Created by swrrt on 1/10/2015.
 */
public class    SVMBolt extends BaseRichBolt{
    OutputCollector collector;
    String filename;
    svm_model model;
    public SVMBolt(String name){
        filename = name;
    }
    @Override
    public void prepare(Map conf,TopologyContext context, OutputCollector _collector){
        collector = _collector;
        try{
            model = svm.svm_load_model(filename);
            svm.svm_save_model(filename+".txt",model);
        }catch (Exception e){}

    }
    @Override
    public void execute(Tuple tuple){
        List<Double> feature = ((List<Double>)tuple.getValueByField("Feature"));
        collector.ack(tuple);
        svm_node [] nodes = new svm_node[feature.size()];
        for(int i=0;i<feature.size();i++){
            svm_node node= new svm_node();
            node.index = i+1;
            node.value = feature.get(i);
            nodes[i] = node;
        }
        int totalClass = model.nr_class;
        int []labels = new int[totalClass];
        svm.svm_get_labels(model,labels);
        System.out.println("[ SVM ]"+String.valueOf(labels.length));
        double[] prob = new double[totalClass];
        double c = svm.svm_predict_probability(model,nodes,prob);
        c = svm.svm_predict(model,nodes);
        try{
            String name = tuple.getStringByField("Filename");
            name = name.substring(0,name.length()-4)+"_"+String.valueOf(tuple.getIntegerByField("Pack"))+".txt";
            FileWriter out = new FileWriter(name,false);
            /*for(int i=0;i<nodes.length;i++){
                out.write(String.valueOf(nodes[i].value)+" ");
            }*/
            out.write(String.valueOf(nodes.length));
            out.write("\n");
            for(int i=0;i<totalClass;i++)out.write(String.valueOf(labels[i])+":"+String.valueOf(prob[i])+" ");
            out.write("\n"+String.valueOf(c)+"\n");
            out.close();
        }catch(Exception e){}
        collector.emit(tuple,new Values(new Double(c),tuple.getStringByField("Filename"),tuple.getIntegerByField("Pack"),tuple.getIntegerByField("Frame"),tuple.getIntegerByField("Patch"),tuple.getIntegerByField("Scale"),tuple.getIntegerByField("sPatch")));
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("Class","Filename","Pack","Frame","Patch","Scale","sPatch"));
    }
}
