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
import javafx.util.Pair;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacpp.opencv_imgproc;
import org.bytedeco.javacpp.opencv_video;
import tool.Serializable;

import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.util.*;

public class OpticalFlowBolt extends BaseRichBolt {
    OutputCollector collector;
    Map<Pair<String,Pair<Integer,Pair<Integer,Pair<Integer,Pair<Integer,Integer>>>>>,Pair<opencv_core.Mat,Long>> bufferl,buffern;  /* The Long has some potential problem */
    Map<Long,Pair<String,Pair<Integer,Pair<Integer,Pair<Integer,Pair<Integer,Integer>>>>>> queuel,queuen;
    int Maxn;
    int side;
    double sigma, thres;
    long n;

    public OpticalFlowBolt(int _side,double _sigma, double _thres) {
        side = _side;
        sigma = _sigma;
        thres = _thres;
    }
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector _collector){
        collector = _collector;
        Maxn = 20000;  /* Buffer 100 images */
        buffern = new HashMap<>();
        bufferl = new HashMap<>();
        queuel = new TreeMap<>();
        queuen = new TreeMap<>();
        n=0;
        System.out.println("!!!!!! !!!!!!!!" + context.getThisComponentId());

    }
    @Override
    public void execute(Tuple tuple) {
        opencv_core.IplImage fkImpage = new opencv_core.IplImage();
        /*  Buffering */
        opencv_core.Mat x = ((tool.Serializable.Mat)tuple.getValueByField("Image")).toJavaCVMat(),y;
        Double [][] Vx = new Double [x.rows()][x.cols()],Vy = new Double[x.rows()][x.cols()];
        String filename = tuple.getStringByField("Filename");
        int pack = tuple.getIntegerByField("Pack"),frame = tuple.getIntegerByField("Frame"),patch = tuple.getIntegerByField("Patch"),scale = tuple.getIntegerByField("Scale"),sPatch = tuple.getIntegerByField("sPatch");
        Pair<String,Pair<Integer,Pair<Integer,Pair<Integer,Pair<Integer,Integer>>>>> last = new Pair<>(filename, new Pair<>(pack, new Pair<>(frame-1,new Pair<>(patch,new Pair<>(scale, sPatch)))));
        Pair<String,Pair<Integer,Pair<Integer,Pair<Integer,Pair<Integer,Integer>>>>> next = new Pair<>(filename, new Pair<>(pack, new Pair<>(frame+1,new Pair<>(patch,new Pair<>(scale, sPatch)))));
        Pair<String,Pair<Integer,Pair<Integer,Pair<Integer,Pair<Integer,Integer>>>>> now = new Pair<>(filename, new Pair<>(pack, new Pair<>(frame,new Pair<>(patch,new Pair<>(scale, sPatch)))));
        boolean fl = false;
        n++;
        if(bufferl.containsKey(last)){
            y = bufferl.get(last).getKey().clone();
            //OpticalFlow(y,x,Vx,Vy);
            CVOpticalFlow(y,x,Vx,Vy);
            //System.out.printf("OF out put: pack %d frame %d\n", pack, frame - 1);
            collector.emit(tuple, new Values(new tool.Serializable.Mat(x), filename, pack, frame - 1, patch, scale, sPatch, Vx, Vy));
            queuel.remove(bufferl.get(last).getValue());
            bufferl.remove(last);
        }else{
            if(queuen.size()>Maxn){
                last = queuen.entrySet().iterator().next().getValue();
                buffern.remove(last);
                queuen.remove(queuen.entrySet().iterator().next().getKey());
            }
            buffern.put(now,new Pair<>(x,n));
            queuen.put(n,now);
        }
        if(buffern.containsKey(next)){
            y = buffern.get(next).getKey().clone();
            //OpticalFlow(x,y,Vx,Vy);
            CVOpticalFlow(x,y,Vx,Vy);
            //System.out.printf("OF out put: pack %d frame %d\n", pack, frame);
            collector.emit(tuple,new Values(new tool.Serializable.Mat(x), filename, pack, frame, patch, scale, sPatch, Vx, Vy));
            queuen.remove(buffern.get(next).getValue());
            buffern.remove(next);
        }else{
            if(queuel.size()>Maxn){
                last = queuel.entrySet().iterator().next().getValue();
                bufferl.remove(last);
                queuel.remove(queuel.entrySet().iterator().next().getKey());
            }
            bufferl.put(now,new Pair<>(x,n));
            queuel.put(n,now);
        }
        collector.ack(tuple);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("Image", "Filename", "Pack", "Frame", "Patch", "Scale", "sPatch", "OFx", "OFy"));
    }
    public void CVOpticalFlow(opencv_core.Mat img, opencv_core.Mat oimg, Double[][] Vx, Double[][] Vy){
        opencv_video.DenseOpticalFlow ofl = opencv_video.createOptFlow_DualTVL1();
        opencv_core.Mat t=new opencv_core.Mat(),gimg = new opencv_core.Mat(),goimg= new opencv_core.Mat();
        opencv_imgproc.cvtColor(img, gimg, opencv_imgproc.COLOR_BGR2GRAY);
        opencv_imgproc.cvtColor(oimg, goimg, opencv_imgproc.COLOR_BGR2GRAY);
        gimg.convertTo(gimg, opencv_core.CV_32FC1);
        goimg.convertTo(goimg, opencv_core.CV_32FC1);

        ofl.calc(gimg, goimg, t);
        //System.out.println(" OF " + t.rows() + " " + t.cols());
        //System.out.println(" OF!"+gimg.rows()+" "+gimg.cols());
        FloatBuffer in = t.getFloatBuffer();
        for(int i=0;i<t.rows();i++)
            for(int j=0;j<t.cols();j++){
                Vx[i][j] = (double)in.get();
                Vy[i][j]= (double)in.get();
                if(Vx[i][j]*Vx[i][j]+Vy[i][j]*Vy[i][j]>thres){
                    Vx[i][j]=0.0;
                    Vy[i][j]=0.0;
                }

            }
    }
    public void OpticalFlow(opencv_core.Mat img, opencv_core.Mat oimg, Double[][] Vx, Double[][] Vy){
        opencv_core.Mat gimg = new opencv_core.Mat(), goimg = new opencv_core.Mat();
        //int side = 5;
        double [][] Ix = new double[img.rows()][img.cols()], Iy = new double[img.rows()][img.cols()], It = new double[img.rows()][img.cols()];
        //Vx = new Double[img.rows()][img.cols()];
        //Vy = new Double[img.rows()][img.cols()];
        opencv_imgproc.cvtColor(img, gimg, opencv_imgproc.COLOR_BGR2GRAY);
        opencv_imgproc.cvtColor(oimg, goimg, opencv_imgproc.COLOR_BGR2GRAY);
        for (int i = 0; i < gimg.rows(); i++) {
            for (int j = 0; j < gimg.cols(); j++) {
                if (i == 0) {
                    Iy[i][j] = (gimg.ptr(i + 1, j).get() - gimg.ptr(i, j).get()) * 2;
                } else if (i == img.rows() - 1) {
                    Iy[i][j] = (gimg.ptr(i, j).get() - gimg.ptr(i - 1, j).get()) * 2;
                } else {
                    Iy[i][j] = (gimg.ptr(i + 1, j).get() - gimg.ptr(i - 1, j).get());
                }
                if (j == 0) {
                    Ix[i][j] = (gimg.ptr(i, j + 1).get() - gimg.ptr(i, j).get()) * 2;
                } else if (j == img.cols() - 1) {
                    Ix[i][j] = (gimg.ptr(i, j).get() - gimg.ptr(i, j - 1).get()) * 2;
                } else {
                    Ix[i][j] = (gimg.ptr(i, j + 1).get() - gimg.ptr(i, j - 1).get());
                }
                It[i][j] = goimg.ptr(i, j).get() - gimg.ptr(i, j).get();
            }
        }
            /* LK Optical Flow */
        double[][] gaussian = new double[side * 2 + 1][side * 2 + 1];
        //double sigma = 1;
        for (int i = 0; i < side * 2 + 1; i++)
            for (int j = 0; j < side * 2 + 1; j++) {
                double dis = (i - 2.0) * (i - 2) + (j - 2) * (j - 2);
                gaussian[i][j] = Math.exp(-dis / (2 * sigma * sigma)) / (sigma * Math.sqrt(Math.PI * 2));
            }
        for (int i = 0; i < img.rows(); i++) {
            for (int j = 0; j < img.cols(); j++) {
                double a = 0, b = 0, c = 0, d = 0, e = 0, f = 0;
                for (int x = 0; x < side * 2 + 1; x++)
                    for (int y = 0; y < side * 2 + 1; y++) {
                        int tx = x + i - side, ty = y + j - side;
                        if (tx >= 0 && tx < img.rows() && ty >= 0 && ty < img.cols()) {
                            a += gaussian[x][y] * Ix[tx][ty] * Ix[tx][ty];
                            b += gaussian[x][y] * Ix[tx][ty] * Iy[tx][ty];
                            c += gaussian[x][y] * Ix[tx][ty] * It[tx][ty];
                            d += gaussian[x][y] * Ix[tx][ty] * Iy[tx][ty];
                            e -= gaussian[x][y] * Iy[tx][ty] * Iy[tx][ty];
                            f -= gaussian[x][y] * Iy[tx][ty] * It[tx][ty];
                        }
                    }
                Vy[i][j] = (f - c * d / a) / (e - b * d / a);
                Vx[i][j] = (c - b * Vy[i][j]) / a;
                if(Vy[i][j]*Vy[i][j]+Vx[i][j]*Vx[i][j]>thres||(Double.isNaN(Vx[i][j]))||(Double.isNaN(Vy[i][j]))){
                    Vy[i][j] =.0;
                    Vx[i][j] =.0;
                }
            }
        }
    }
}