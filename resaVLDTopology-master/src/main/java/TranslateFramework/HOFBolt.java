package TranslateFramework;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.*;

import javax.xml.bind.ValidationEvent;
import java.io.FileWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by swrrt on 14/9/2015.
 */
public class HOFBolt extends BaseRichBolt{
    OutputCollector collector;
    int ncell,nbin;
    boolean is_single;
    public HOFBolt(int _ncell,int _nbin, boolean _is_single){
        ncell = _ncell;
        nbin = _nbin;
        is_single = _is_single;
    }
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector _collector){
        collector = _collector;

    }
    @Override
    public void execute(Tuple tuple){
        opencv_core.IplImage fkImpage = new opencv_core.IplImage();
        //opencv_core.Mat img = ((tool.Serializable.Mat)tuple.getValueByField("Image")).toJavaCVMat(), oimg = ((tool.Serializable.Mat)tuple.getValueByField("NImage")).toJavaCVMat();
        List<Double[][]> Vxs,Vys;
        if(tuple.contains("OFxs")){
            Vxs = ((List<Double[][]>)tuple.getValueByField("OFxs"));
            Vys = ((List<Double[][]>)tuple.getValueByField("OFys"));
        }else{
            Vxs = new ArrayList<>();
            Vxs.add((Double[][])tuple.getValueByField("OFx"));
            Vys = new ArrayList<>();
            Vys.add((Double[][])tuple.getValueByField("OFy"));
        }
        collector.ack(tuple);
        List<Double> feature = new ArrayList<>();
        List<List<Double>> HOF = new ArrayList<>();

        /* Optical Flow Over */
        for(int tt=0;tt<Vxs.size();tt++) {
            Double[][] Vx = Vxs.get(tt), Vy = Vys.get(tt);
            if (!is_single) {
                //List<Double> feature;
                int t=0;
                for (int i = ncell; i < Vx.length - ncell; i++)
                    for (int j = ncell; j < Vx[0].length - ncell; j++) {
                        double ss = 0;
                        double[] a = new double[nbin];
                        for (int x = i - ncell; x <= i + ncell; x++)
                            for (int y = j - ncell; y <= j + ncell; y++) {
                                if (true) {
                                    double ag = Math.atan2(Vy[x][y], Vx[x][y]);

                                    if (ag < 0) ag += Math.PI * 2;
                                    double xx = Math.sqrt(Vx[x][y] * Vx[x][y] + Vy[x][y] * Vy[x][y]);
                                    a[(int) Math.floor(nbin * ag / (Math.PI * 2))] += xx;
                                    ss += xx;
                                }
                            }
                        if (Math.abs(ss) < 1e-9) ss = 1;
                        feature = new ArrayList<>();
                        for (int x = 0; x < nbin; x++) {
                            a[x] /= ss;
                            feature.add(a[x]);
                        }
                        if(HOF.size()<=t){
                            HOF.add(feature);
                        }else HOF.get(t).addAll(feature);
                        t++;
                    }
                //collector.emit(tuple, new Values(HOF, tuple.getStringByField("Filename"), tuple.getIntegerByField("Pack"), tuple.getIntegerByField("Frame"), tuple.getIntegerByField("Patch"), tuple.getIntegerByField("Scale"), tuple.getIntegerByField("sPatch")));
            } else {
                double ss = 0;
                double[] a = new double[nbin];
                int i = Vx.length / 2;
                int j = Vx[0].length / 2;
                for (int x = i - ncell; x <= i + ncell; x++)
                    for (int y = j - ncell; y <= j + ncell; y++) {
                        if (true) {
                            double ag = Math.atan2(Vy[x][y], Vx[x][y]);
                            if (ag < -1e-9) ag += Math.PI * 2;
                            else if (ag < 0) ag = 0;
                            double xx = Math.sqrt(Math.abs(Vx[x][y] * Vx[x][y] + Vy[x][y] * Vy[x][y]));
                            a[(int) Math.floor(nbin * ag / (Math.PI * 2))] += xx;
                        }
                    }
                for(int x=0;x<nbin;x++)ss+=a[x]*a[x];
                if (Math.abs(ss) < 1e-9) ss = 1;
                ss = Math.sqrt(ss);
                for (int x = 0; x < nbin; x++) {
                    a[x] /= ss;
                    feature.add(a[x]);
                }
                //System.out.println(" [ HOF ] "+feature);
                //collector.emit(tuple, new Values(feature, "HOF", tuple.getStringByField("Filename"), tuple.getIntegerByField("Pack"), tuple.getIntegerByField("Frame"), tuple.getIntegerByField("Patch"), tuple.getIntegerByField("Scale"), tuple.getIntegerByField("sPatch")));
            }
        }
        collector.emit(tuple, new Values(feature, "HOF", tuple.getStringByField("Filename"), tuple.getIntegerByField("Pack"), tuple.getIntegerByField("Frame"), tuple.getIntegerByField("Patch"), tuple.getIntegerByField("Scale"), tuple.getIntegerByField("sPatch")));
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("Feature","F_type","Filename","Pack","Frame","Patch","Scale","sPatch"));
    }
}
