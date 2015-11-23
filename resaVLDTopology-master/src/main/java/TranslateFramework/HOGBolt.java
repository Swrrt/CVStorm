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
 * Created by swrrt on 9/9/2015.
 */
public class HOGBolt extends BaseRichBolt{
    OutputCollector collector;
    int ncell = 5, nbin = 8;
    boolean ispatch = false;
    public HOGBolt(int _ncell, int _nbin, boolean _ispatch){
        ncell = _ncell;
        nbin = _nbin;
        ispatch = _ispatch;
    }
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector _collector){
        collector = _collector;

    }
    @Override
    public void execute(Tuple tuple){
        opencv_core.IplImage fkImpage = new opencv_core.IplImage();
        List<tool.Serializable.Mat> simgs;
        if(!tuple.contains("Images")){
            simgs = new ArrayList<>();
            simgs.add((tool.Serializable.Mat)tuple.getValueByField("Image"));
        }else simgs = ((List<tool.Serializable.Mat>)tuple.getValueByField("Images"));
        List<opencv_core.Mat> imgs = new ArrayList<>();
        List<Double> output = new ArrayList<>();
        List<List<Double>> feature = new ArrayList<>();
        List<Double> A = new ArrayList<>();
        for(int tt=0;tt<simgs.size();tt++) {
            opencv_core.Mat img = simgs.get(tt).toJavaCVMat(), gimg = new opencv_core.Mat();
            opencv_imgproc.cvtColor(img, gimg, opencv_imgproc.CV_BGR2GRAY);
            double[][] gradx = new double[img.rows()][img.cols() - 2], grady = new double[img.rows() - 2][img.cols()];
            //System.out.println(" first ");
            for (int i = 0; i < img.rows(); i++) {
                for (int j = 0; j < img.cols(); j++) {
                    if (i < img.rows() - 2) {
                        grady[i][j] = -gimg.ptr(i, j).get() + gimg.ptr(i + 2, j).get();
                    }
                    if (j < img.cols() - 2) {
                        gradx[i][j] = -gimg.ptr(i, j).get() + gimg.ptr(i, j + 2).get();
                    }
                }
            }
            //System.out.println(" second ");
            if (!ispatch) {
                int ti=0;
                for (int i = 0; i < (img.rows() - 2) / ncell; i++) {
                    for (int j = 0; j < (img.cols() - 2) / ncell; j++) {
                        double[] vote = new double[nbin];
                        A = new ArrayList<>();
                        for (int t1 = 0; t1 < nbin; t1++)
                            vote[t1] = 0;
                        for (int x = 1; x <= ncell; x++) {
                            for (int y = 1; y <= ncell; y++) {
                                //System.out.printf("[ yoyo ] %d %d %d %d %d %d\n" ,img.rows(),img.cols(),i,j,x,y);
                                double px = gradx[i * ncell + y][j * ncell + x - 1], py = grady[i * ncell + y - 1][j * ncell + x];
                                double grad = Math.sqrt(Math.abs(px * px + py * py));
                                double angle = Math.atan2(py, px);
                                while (angle < 0) angle += 2 * Math.PI;
                                while (angle > 2 * Math.PI) angle -= 2 * Math.PI;
                                vote[(int) (angle * nbin / (Math.PI * 2))] += grad;
                                //System.out.printf("[ yoyo ] %d %d %d %d %d %d\n" ,img.rows(),img.cols(),i,j,x,y);
                            }
                        }
                        int s = 0;
                        for (int k = 0; k < nbin; k++) {
                            s += vote[k]*vote[k];
                        }
                        if (s == 0) s = 1;
                        for (int k = 0; k < nbin; k++) {
                            vote[k] /= Math.sqrt(s);
                            A.add(vote[k]);
                        }
                        if(feature.size()<=ti){
                            feature.add(A);
                        }else feature.get(ti).addAll(A);
                        ti++;
                    }
                }
            /*try {
                FileWriter x = new FileWriter("C:\\testing\\SIFTDEMO\\output\\" + tuple.getIntegerByField("ID") + "_hog.txt", false);
                for (int i = 0; i < nbin; i++) x.write(String.valueOf(feature.get(0).get(i)) + " ");
                x.write("\n");
                for (int i = 0; i < nbin; i++) x.write(String.valueOf(feature.get(1).get(i)) + " ");
                x.write("\n");
                x.close();
            } catch (Exception e) {
            }
            opencv_highgui.imwrite("C:/testing/SIFTDEMO/output/" + String.valueOf(tuple.getIntegerByField("ID")) + ".jpg", gimg);
            collector.emit(new Values(tuple.getIntegerByField("ID"), new tool.Serializable.Mat(img), feature, tuple.getBooleanByField("IsCodebook"), tuple.getBooleanByField("IsTraining"), tuple.getIntegerByField("Label")));*/
                //collector.emit(tuple,new Values(feature,tuple.getStringByField("Filename"),tuple.getIntegerByField("Pack"),tuple.getIntegerByField("Frame"),tuple.getIntegerByField("Patch"),tuple.getIntegerByField("Scale"),tuple.getIntegerByField("sPatch")));

            } else {
                double[] vote = new double[nbin];
                int i = 0;
                int j = 0;
                //List<Double> A = new ArrayList<>();
                for (int t1 = 0; t1 < nbin; t1++)
                    vote[t1] = 0;
                for (int x = 1; x <= ncell; x++) {
                    for (int y = 1; y <= ncell; y++) {
                        //System.out.printf("[ yoyo ] %d %d %d %d %d %d\n" ,img.rows(),img.cols(),i,j,x,y);
                        double px = gradx[i * ncell + y][j * ncell + x - 1], py = grady[i * ncell + y - 1][j * ncell + x];
                        double grad = Math.sqrt(Math.abs(px * px + py * py));
                        double angle = Math.atan2(py, px);
                        while (angle < 0) angle += 2 * Math.PI;
                        while (angle > 2 * Math.PI) angle -= 2 * Math.PI;
                        vote[(int) (angle * nbin / (Math.PI * 2))] += grad;
                        //System.out.printf("[ yoyo ] %d %d %d %d %d %d\n" ,img.rows(),img.cols(),i,j,x,y);
                    }
                }
                double s = 0;
                for (int k = 0; k < nbin; k++) {
                    s += vote[k];
                }
                if (s == 0) s = 1;
                for (int k = 0; k < nbin; k++) {
                    vote[k] /= s;
                    A.add(vote[k]);
                }
                //System.out.println("[ HOG ] "+A);
                //collector.emit(tuple,new Values(A,"HOG",tuple.getStringByField("Filename"),tuple.getIntegerByField("Pack"),tuple.getIntegerByField("Frame"),tuple.getIntegerByField("Patch"),tuple.getIntegerByField("Scale"),tuple.getIntegerByField("sPatch")));
            }

        }
        if(ispatch)collector.emit(tuple,new Values(A,"HOG",tuple.getStringByField("Filename"),tuple.getIntegerByField("Pack"),tuple.getIntegerByField("Frame"),tuple.getIntegerByField("Patch"),tuple.getIntegerByField("Scale"),tuple.getIntegerByField("sPatch")));
        collector.ack(tuple);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("Feature","F_type","Filename","Pack","Frame","Patch","Scale","sPatch"));
    }
}
