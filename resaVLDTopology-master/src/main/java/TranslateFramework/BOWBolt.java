package TranslateFramework;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by swrrt on 30/9/2015.
 */
public class BOWBolt extends BaseRichBolt{
    String filename;
    OutputCollector collector;
    int dim,n;
    String name;
    public BOWBolt(String _filename){
        filename = _filename;
    }
    Double[][] dict;
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector _collector){
        collector = _collector;
        name = context.getThisComponentId();
        File file = new File(filename);
        if(file.canRead()){
            try {
                Scanner in = new Scanner(file);

                n = in.nextInt();
                dim = in.nextInt();
                dict = new Double[n][dim];
                for (int i = 0; i < n; i++) {
                    for (int j = 0; j < dim; j++) {
                        dict[i][j] = (in.nextDouble());
                    }

                }
            }catch (Exception e){}
        }
    }
    @Override
    public void execute(Tuple tuple){
        List<List<Double>> features = (List<List<Double>>)tuple.getValueByField("Features");
        double[] a = new double[n];
        double ss=0;
        System.out.println(" BOW !!!");
        for(int i=0;i<features.size();i++){
            int mini=0;
            System.out.println("!!!!! "+String.valueOf(i));
            double min = 1e100;
            for(int j=0;j<n;j++){
                double s=0;
                for(int k=0;k<dim;k++){
                    s+=(features.get(i).get(k)-dict[j][k])*(features.get(i).get(k)-dict[j][k]);
                }
                if(s<min){
                    min=s;
                    mini=j;
                }
            }
            a[mini]++;
            ss+=a[mini]*2-1;
        }
        List<Double> feature = new ArrayList<>();
        ss = Math.sqrt(ss);
        for(int i=0;i<n;i++){
            a[i]/=ss;
            feature.add(a[i]);
        }
        collector.emit(tuple,new Values(feature,"BOW_"+name,tuple.getStringByField("Filename"),tuple.getIntegerByField("Pack"),tuple.getIntegerByField("Frame"),tuple.getIntegerByField("Patch"),tuple.getIntegerByField("Scale"),tuple.getIntegerByField("sPatch")));
        collector.ack(tuple);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("Feature","F_type","Filename","Pack","Frame","Patch","Scale","sPatch"));
    }
}
