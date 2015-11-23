package TranslateFramework;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by swrrt on 29/9/2015.
 */
public class MBHBolt extends BaseRichBolt{
    OutputCollector collector;
    int ncell,nbin;
    boolean is_single;
    public MBHBolt(int _ncell,int _nbin, boolean _is_single){
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
        List<List<Double>> HOF = new ArrayList<>();
        List<Double> feature = new ArrayList<>();
        for(int tt=0;tt<Vxs.size();tt++) {
            Double[][] Vx = Vxs.get(tt), Vy = Vys.get(tt);
            Double[][] Dxx = new Double[Vx.length][Vx[0].length], Dxy = new Double[Vx.length][Vx[0].length], Dyx = new Double[Vx.length][Vx[0].length], Dyy = new Double[Vx.length][Vx[0].length];
            for (int i = 0; i < Vx.length; i++)
                for (int j = 0; j < Vx[0].length; j++) {
                    if (i > 0 && i < Vx.length - 1) {
                        Dxy[i][j] = -Vx[i - 1][j] + Vx[i + 1][j];
                        Dyy[i][j] = -Vy[i - 1][j] + Vy[i + 1][j];
                    }
                    if (j > 0 && j < Vx[0].length - 1) {
                        Dxx[i][j] = -Vx[i][j - 1] + Vx[i][j + 1];
                        Dyx[i][j] = -Vy[i][j - 1] + Vy[i][j + 1];
                    }
                }
        /* Optical Flow Over */
            if (!is_single) {
            /* TODO */
                //List<List<Double>> HOF = new ArrayList<>();
                //List<Double> feature;
                int ts=0;
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

                                }
                            }
                        for(int x=0;x<nbin;x++)ss+=a[x]*a[x];
                        if (Math.abs(ss) < 1e-9) ss = 1;
                        ss = Math.sqrt(ss);
                        feature = new ArrayList<>();
                        for (int x = 0; x < nbin; x++) {
                            a[x] /= ss;
                            feature.add(a[x]);
                        }
                        if(HOF.size()<=ts)
                            HOF.add(feature);
                        else HOF.get(ts).addAll(feature);
                        ts++;
                    }
                //collector.emit(tuple, new Values(HOF, tuple.getStringByField("Filename"), tuple.getIntegerByField("Pack"), tuple.getIntegerByField("Frame"), tuple.getIntegerByField("Patch"), tuple.getIntegerByField("Scale"), tuple.getIntegerByField("sPatch")));
            } else {
                //List<Double> feature;
                double ss = 0;
                double[] a = new double[nbin];
                for (int x = 1; x < Vx.length - 1; x++)
                    for (int y = 1; y < Vx[0].length - 1; y++) {
                        if (true) {
                            double ag = Math.atan2(Dxy[x][y], Dxx[x][y]);
                            if (ag < -1e-9) ag += Math.PI * 2;
                            else if (ag < 0) ag = 0;
                            double xx = Math.sqrt(Math.abs(Dxx[x][y] * Dxx[x][y] + Dxy[x][y] * Dxy[x][y]));
                            if (Double.isNaN(xx)) {
                                System.out.println("[ ZZ ] " + Dxx[x][y] + "   |||    " + Dxy[x][y]);
                            }
                            a[(int) Math.floor((nbin / 2) * ag / (Math.PI * 2))] += xx;
                            ss += xx;
                            ag = Math.atan2(Dyy[x][y], Dyx[x][y]);
                            if (ag < -1e-9) ag += Math.PI * 2;
                            else if (ag < 0) ag = 0;
                            xx = Math.sqrt(Math.abs(Dyx[x][y] * Dyx[x][y] + Dyy[x][y] * Dyy[x][y]));
                            if (Double.isNaN(xx)) {
                                System.out.println("[ ZZ ] " + Dyx[x][y] + "   |||    " + Dyy[x][y]);
                            }
                            a[nbin / 2 + ((int) Math.floor((nbin / 2) * ag / (Math.PI * 2)))] += xx;
                            ss += xx;
                        }
                    }
                if (Math.abs(ss) < 1e-9) ss = 1;
                feature = new ArrayList<>();
                for (int x = 0; x < nbin; x++) {
                    a[x] /= ss;
                    feature.add(a[x]);
                }
                //System.out.println(" [ MBH ] "+ss+ " ||| "+feature);
                //collector.emit(tuple, new Values(feature, "MBH", tuple.getStringByField("Filename"), tuple.getIntegerByField("Pack"), tuple.getIntegerByField("Frame"), tuple.getIntegerByField("Patch"), tuple.getIntegerByField("Scale"), tuple.getIntegerByField("sPatch")));
            }
        }
        collector.emit(new Values(feature, "MBH", tuple.getStringByField("Filename"), tuple.getIntegerByField("Pack"), tuple.getIntegerByField("Frame"), tuple.getIntegerByField("Patch"), tuple.getIntegerByField("Scale"), tuple.getIntegerByField("sPatch")));
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("Feature","F_type","Filename","Pack","Frame","Patch","Scale","sPatch"));
    }
}
