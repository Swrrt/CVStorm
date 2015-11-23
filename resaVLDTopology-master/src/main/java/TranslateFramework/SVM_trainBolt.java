package TranslateFramework;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import libsvm.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by swrrt on 1/10/2015.
 */
public class SVM_trainBolt extends BaseRichBolt{
    OutputCollector collector;
    String filename;
    svm_problem problem;
    int limit,now;
    svm_parameter parameter;
    public SVM_trainBolt(String name,int _limit){
        filename = name;
        limit = _limit;


    }
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector _collector){
        collector = _collector;
        problem = new svm_problem();
        problem.l = limit;
        problem.x = new svm_node[limit][];
        problem.y = new double[limit];
        parameter = new svm_parameter();
        parameter.probability = 1;
        parameter.gamma = 0.5;
        parameter.nu = 0.5;
        parameter.C = 1;
        parameter.svm_type = svm_parameter.C_SVC;
        parameter.kernel_type = svm_parameter.RBF;
        parameter.cache_size = 1024;
        parameter.eps = 0.0001;
        now=0;
    }
    @Override
    public void execute(Tuple tuple){
        if(now>=limit)return ;
        List<Double> feature = (List<Double>)tuple.getValueByField("Feature");
        int label=0;
        for(int i=0;i<10;i++)
            if(tuple.getStringByField("Filename").contains("label"+"_"+String.valueOf(i)))label = i;
        System.out.println(tuple.getStringByField("Filename")+" label is "+String.valueOf(label));
        problem.x[now] = new svm_node[feature.size()];
        for(int i=0;i<feature.size();i++){
            problem.x[now][i] = new svm_node();
            problem.x[now][i].value = feature.get(i);
            problem.x[now][i].index=i;
        }
        problem.y[now]=(double)label;
        now++;
        System.out.println("[ SVM training ] "+ now);
        if(now>=limit){
            svm_model model = svm.svm_train(problem,parameter);
            try{
                svm.svm_save_model(filename,model);
            }catch(Exception e){}
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
    }
}
