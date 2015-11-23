package TranslateFramework;


import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by swrrt on 21/9/2015.
 */
public class SURFBolt extends BaseBasicBolt {
    public double cal_I(double[][] I, int rx,int ry, int lx,int ly){
        double s = I[rx][ry];
        if(lx>0)s -= I[lx-1][ry];
        if(ly>0)s -= I[rx][ly-1];
        if(lx>0&&ly>0)s -= I[lx-1][ly-1];
        return s;
    }
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        opencv_core.IplImage fkImpage = new opencv_core.IplImage();
        opencv_core.Mat img = ((tool.Serializable.Mat) tuple.getValueByField("Image")).toJavaCVMat().clone(), outputimg = img.clone(), gimg = new opencv_core.Mat();
        opencv_imgproc.cvtColor(img, gimg, opencv_imgproc.COLOR_BGR2GRAY);
        double[][] I = new double[gimg.rows()][gimg.cols()];
        List<double[][]> Hes = new ArrayList<>();
        int scalen = 0;
        int x = 3;
        while (x <= gimg.rows() && x <= gimg.cols()) {
            scalen++;
            x = (int)Math.ceil(x*1.2);
        }
        int[] scale = new int[scalen];
        x = 3;
        for (int i = 0; i < scalen; i++) {
            scale[i] = x;
            Hes.add(new double[gimg.rows() + 1 - x * 3][gimg.cols() + 1 - x * 3]);
            x = (int) Math.ceil(x * 1.2);
        }
        for (int i = 0; i < gimg.rows(); i++)
            for (int j = 0; j < gimg.cols(); j++) {
                I[i][j] = (double) gimg.ptr(i, j).get();
                if (i > 0) I[i][j] += I[i - 1][j];
                if (j > 0) I[i][j] += I[i][j - 1];
                if (i > 0 && j > 0) I[i][j] -= I[i - 1][j - 1];
            }
        double omega = 0.9;
        for (int s = 0; s < scalen; s++)
            for (int i = 0; i < gimg.rows() + 1 - scale[s]*3; i++) {
                for(int j = 0; j < gimg.cols() + 1 - scale[s]*3;j++){
                    double Dxx,Dyy,Dxy;
                    int lx = j + scale[s] - 1 , rx = j + scale[s]*2 - scale[s]/2;
                    int uy = i + scale[s]/3 , dy = i + scale[s] * 2 - scale[s]/3;
                    Dyy = cal_I(I,uy-1,rx,i,lx) - 2*cal_I(I,dy,rx,uy,lx) + cal_I(I,i+scale[s]*2,rx,dy+1,lx);
                    uy = i + scale[s]/2;
                    dy = i + scale[s]*2 - scale[s]/2;
                    lx = j + scale[s]/3;
                    rx = j + scale[s] * 2 - scale[s]/3;
                    Dxx = cal_I(I,dy,lx-1,uy,j) - 2*cal_I(I,dy,rx,uy,lx) + cal_I(I,dy,j+scale[s]*2,uy,rx+1);
                    Dxy = 0;
                    Hes.get(s)[i][j] = Dxx*Dyy - Dxy*Dxy*omega;
                }
            }
            /* TODO */
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){

    }
}
