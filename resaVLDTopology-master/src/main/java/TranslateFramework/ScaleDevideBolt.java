package TranslateFramework;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;
import tool.Serializable;

import java.util.Map;

/**
 * Created by swrrt on 22/10/2015.
 */
public class ScaleDevideBolt extends BaseRichBolt {
    double ratio;
    int number;
    OutputCollector collector;
    public ScaleDevideBolt(double r,int n){
        ratio = r;
        number = n;
    }
    @Override
    public void prepare(Map conf,TopologyContext context, OutputCollector _collector){
        opencv_core.IplImage fkImpage = new opencv_core.IplImage();
        collector = _collector;
    }
    @Override
    public void execute(Tuple tuple){
        collector.ack(tuple);
        opencv_core.Mat img = ((Serializable.Mat)tuple.getValueByField("Image")).toJavaCVMat(),timg = new opencv_core.Mat();
        int w=img.cols(),h=img.rows();
        for(int i=0;i<number;i++){
            opencv_imgproc.resize(img, timg, new opencv_core.Size(w, h));
            collector.emit(tuple,new Values(new Serializable.Mat(timg),tuple.getStringByField("Filename"),tuple.getIntegerByField("Pack"),tuple.getIntegerByField("Frame"),tuple.getIntegerByField("Patch"),i,tuple.getIntegerByField("sPatch")));
            double tw = w/ratio,th = h/ratio;
            w = (int)Math.round(tw);
            h = (int)Math.round(th);
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        opencv_core.IplImage fkImpage = new opencv_core.IplImage();
        declarer.declare(new Fields("Image","Filename","Pack","Frame","Patch","Scale","sPatch"));
    }
}
