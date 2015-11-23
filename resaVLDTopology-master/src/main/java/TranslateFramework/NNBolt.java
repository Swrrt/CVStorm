package TranslateFramework;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import logodetection.Debug;
import org.bytedeco.javacpp.opencv_core;
import tool.Serializable;

import java.io.File;
import java.io.FileWriter;
import java.rmi.server.ExportException;
import java.util.*;
import tool.Serializable.*;
/**
 * Created by swrrt on 8/9/2015.
 */
public class NNBolt extends BaseRichBolt{
    List<List<Double>>trainSet;
    List<Integer>trainLabel;
    List<double[][]>W;
    List<double[]>B;
    List<Integer> N;
    int n_layer;
    boolean training = true;
    OutputCollector collector;
    public double sigmod(double x){
        return 1.0/(1+Math.exp(-x));
    }
    public double sigmod_d(double x){
        return Math.exp(x)/((Math.exp(x)+1)*(Math.exp(x)+1));
    }
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector _collector){
        collector = _collector;
        opencv_core.IplImage fkImpage = new opencv_core.IplImage();
        trainSet = new ArrayList<>();
        trainLabel = new ArrayList<>();
        W = new ArrayList<>();
        B = new ArrayList<>();
        N = new ArrayList<>();
        n_layer = 4;
        File x = new File("C:\\testing\\SIFTDEMO\\trainedNN.dat");
        if(x.exists()){
            try{
                Scanner in = new Scanner(x);
                n_layer = in.nextInt();
                for(int i=0;i<n_layer-1;i++){
                    int n,m;
                    n = in.nextInt();
                    N.add(n);
                    m = in.nextInt();
                    if(i==n_layer-2)
                        N.add(m);
                    double[][] tw = new double[m][n];
                    for(int j=0;j<m;j++) {
                        for (int k = 0; k < n; k++) {
                            tw[j][k] = in.nextDouble();
                        }
                    }
                    W.add(tw);
                    double[] tb = new double[m];
                    for(int j=0;j<m;j++)
                        tb[j] = in.nextDouble();
                    B.add(tb);
                }
            }catch (Exception ex){};
            training = false;
        }else{
            training = true;
        }
    }
    @Override
    public void execute(Tuple tuple){
        boolean istraining = tuple.getBooleanByField("IsTraining");
        System.out.printf("[ NN ]  What happened? %d\n",tuple.getIntegerByField("ID"));
        if(istraining&&training){
            trainSet.add((List<Double>)tuple.getValueByField("Feature"));
            trainLabel.add(tuple.getIntegerByField("Label"));
        }else if(training){
            N.add(trainSet.get(0).size());
            N.add(100);
            N.add(50);
            N.add(1);
            List<double[][]> deltaw = new ArrayList<>();
            List<double[]>deltab = new ArrayList<>();
            for(int i=1;i<n_layer;i++){
                double[][] tw = new double[N.get(i)][N.get(i-1)];
                double[] tb = new double[N.get(i-1)];
                Random rand = new Random();
                for(int x=0;x<N.get(i-1);x++) {
                    for (int y = 0; y < N.get(i); y++)
                        tw[y][x] = (rand.nextInt(10000)) / 5000.0 - 1.0;
                    tb[x] = rand.nextInt(10000) / 5000.0 - 1.0;
                }
                deltaw.add(new double[N.get(i)][N.get(i-1)]);
                W.add(tw);
                deltab.add(new double[N.get(i-1)]);
                B.add(tb);
            }
            List<double[]> in = new ArrayList<>(),out = new ArrayList<>(), delta = new ArrayList<>();
            for(int i=0;i<n_layer;i++){
                in.add(new double[N.get(i)]);
                out.add(new double[N.get(i)]);
                delta.add(new double[N.get(i)]);
            }
            /* construction complete! */
            /* training! */
            int iter = 1000;
            double alpha = 0.5, gamma = 0.0001, error;
            while(iter>0){
                System.out.println("[ NeuralNetwork training ] iter = "+iter);
                iter--;
                error = 0;
                for(int i=0;i<n_layer-1;i++)
                    for(int j=0;j<N.get(i+1);j++) {
                        for (int k = 0; k < N.get(i); k++)
                            deltaw.get(i)[j][k] = 0.0;
                        deltab.get(i)[j] = 0.0;
                    }
                for(int tt=0;tt<trainSet.size();tt++){
                    /* Forward Propagating */
                    for(int j=0;j<N.get(0);j++){
                        out.get(0)[j] = trainSet.get(tt).get(j) ;
                    }
                    for(int i=1;i<n_layer;i++){
                        for(int j=0;j<N.get(i);j++){
                            in.get(i)[j] = B.get(i-1)[j];
                            for(int k=0;k<N.get(i-1);k++) {
                                in.get(i)[j] += W.get(i-1)[j][k] * out.get(i - 1)[k];
                            }
                            out.get(i)[j] = sigmod(in.get(i)[j]);
                        }
                    }
                    /* Back propagating */
                    for(int j=0;j<N.get(n_layer-1);j++){
                        delta.get(n_layer-1)[j] = -sigmod_d(in.get(n_layer-1)[j])*(trainLabel.get(tt)-out.get(n_layer-1)[j]);
                        double eps = 1e-5,re = 0,p1,p2;
                        p1 = trainLabel.get(tt)-sigmod(in.get(n_layer-1)[j]+eps);
                        p2 = trainLabel.get(tt)-sigmod(in.get(n_layer-1)[j]-eps);
                        re = (p1*p1-p2*p2)/(4*eps);
                        //System.out.printf("[ NeuralNetwork Debug ] %.10f %.10f %.10f\n", delta.get(n_layer-1)[j], re, Math.abs(delta.get(n_layer-1)[j]-re));
                        error += (trainLabel.get(tt)-out.get(n_layer-1)[j])*(trainLabel.get(tt)-out.get(n_layer-1)[j]);
                    }
                    for(int i = n_layer-2;i>0;i--){
                        for(int j=0;j<N.get(i);j++){
                            double s =0;
                            for(int k=0;k<N.get(i+1);k++){
                                s += W.get(i)[k][j]*delta.get(i+1)[k];
                            }
                            delta.get(i)[j] = s*sigmod_d(in.get(i)[j]);
                        }
                    }
                    for(int i=0;i<n_layer-1;i++){
                        for(int j=0;j<N.get(i+1);j++) {
                            for (int k = 0; k < N.get(i); k++) {
                                deltaw.get(i)[j][k] += out.get(i)[k] * delta.get(i + 1)[j];
                            }
                            deltab.get(i)[j] += delta.get(i + 1)[j];
                        }
                    }

                }
                /* Gradient Descent */
                for(int i=0;i<n_layer-1;i++){
                    for(int j=0;j<N.get(i+1);j++){
                        for(int k=0;k<N.get(i);k++)
                            W.get(i)[j][k] -= alpha*(deltaw.get(i)[j][k]/trainSet.size()+gamma*W.get(i)[j][k]);
                        B.get(i)[j] -= alpha*(deltab.get(i)[j]/trainSet.size());
                    }
                }
                System.out.println("[ NeuralNetwork Training ] Error = "+ error);
            }
            try{
                FileWriter file = new FileWriter("C:\\testing\\SIFTDEMO\\trainedNN.dat");
                file.write(String.valueOf(n_layer));
                file.write("\n");
                for(int i=0;i<n_layer-1;i++){
                    file.write(String.valueOf(N.get(i)));
                    file.write(" ");
                    file.write(String.valueOf(N.get(i+1)));
                    file.write("\n");
                    for(int j=0;j<N.get(i+1);j++) {
                        for (int k = 0; k < N.get(i); k++) {
                            file.write(String.valueOf(W.get(i)[j][k]));
                            file.write(" ");
                        }
                        file.write("\n");
                    }
                    for(int j=0;j<N.get(i+1);j++){
                        file.write(String.valueOf(B.get(i)[j])+" ");
                    }
                    file.write("\n");
                }
                file.close();
            }catch (Exception e){};
            training = false;
        }
        if(!istraining){
            List<Double> input = (List<Double>)tuple.getValueByField("Feature");
            List<double[]> in = new ArrayList<>(),out = new ArrayList<>();
            for(int i=0;i<n_layer;i++){
                in.add(new double[N.get(i)]);
                out.add(new double[N.get(i)]);
            }
            for(int i=0;i<N.get(0);i++){
                out.get(0)[i] = input.get(i) * 100.0;
            }
            for(int i=0;i<n_layer-1;i++){
                for(int j=0;j<N.get(i+1);j++){
                    in.get(i+1)[j] = B.get(i)[j];
                    for(int k=0;k<N.get(i);k++)
                        in.get(i+1)[j]+=W.get(i)[j][k]*out.get(i)[k];
                    out.get(i+1)[j]= sigmod(in.get(i+1)[j]);
                }
            }
            try{
                FileWriter x = new FileWriter("C:\\testing\\SIFTDEMO\\output\\"+tuple.getIntegerByField("ID")+"class.txt");
                x.write(String.valueOf(out.get(n_layer-1)[0]));
                x.close();
            }catch (Exception e){};
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){

    }
}
