package TranslateFramework;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by swrrt on 1/10/2015.
 */
public class BOW_trainBolt extends BaseRichBolt {
    OutputCollector collector;
    String filename;
    int limit;
    List<List<Double>> dict;
    List<List<Double>> center;
    int dim,n;
    boolean over;
    int iter;
    public BOW_trainBolt(String _filename,int _limit,int _n,int _iter){
        filename = _filename;
        limit = _limit;
        n = _n;
        iter = _iter;
    }
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector _collector){
        collector = _collector;
        dict = new ArrayList<>();
        center = new ArrayList<>();
        over =false;
    }
    @Override
    public void execute(Tuple tuple){
        if(over)return ;
        List<List<Double>> input = (List<List<Double>>)tuple.getValueByField("Features");
        collector.ack(tuple);
        for(int i=0;i<input.size();i++){
            dict.add(input.get(i));
            dim = input.get(i).size();
        }
        System.out.println(dict.size()+ " "+dim);
        if(dict.size()>=limit){
            over = true;
            Random rand = new Random();
            for(int i=0;i<n;i++){
                List<Double> a = new ArrayList<>();
                for(int j=0;j<dim;j++){
                    a.add(rand.nextInt(100003)/100003.0);
                }
                center.add(a);
            }
            double[][] sx = new double[n][dim];
            int [] ss = new int[n];
            while(iter>0){
                iter--;
                System.out.println("[ BOW iter ] "+String.valueOf(iter));
                for(int i=0;i<dict.size();i++){
                    int mini=0;
                    double min=1e100;
                    for(int j=0;j<n;j++){
                        double dis=0;
                        for(int k=0;k<dim;k++){
                            dis+=(dict.get(i).get(k)-center.get(j).get(k))*(dict.get(i).get(k)-center.get(j).get(k));
                        }
                        if(dis<min){
                            min = dis;
                            mini = j;
                        }
                    }
                    ss[mini]++;
                    for(int k=0;k<dim;k++)sx[mini][k]+=dict.get(i).get(k);
                }
                for(int j=0;j<n;j++){
                    if(ss[j]>0){
                        List<Double> a =new ArrayList<>();
                        for(int k=0;k<dim;k++){
                            a.add(sx[j][k]/ss[j]);
                        }
                        center.set(j,a);
                    }
                }
                n = center.size();
            }
            try{
                FileWriter out = new FileWriter(filename);
                out.write(String.valueOf(n)+" "+String.valueOf(dim)+"\n");
                for(int i=0;i<n;i++){
                    for(int j=0;j<dim;j++)out.write(String.valueOf(center.get(i).get(j))+" ");
                    out.write("\n");
                }
                out.close();
            }catch (Exception e){}
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){}
}
