package TranslateFramework;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import javafx.util.Pair;
import org.bytedeco.javacpp.opencv_core;
import tool.Serializable;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by swrrt on 21/10/2015.
 */
public class DrawSVMBolt extends BaseRichBolt {
    OutputCollector collector;
    Map <Pair<String,Integer>, Map<Integer,opencv_core.Mat>> buffer;
    Map <Pair<String,Integer>, Double> ClassBuffer;
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector _collector){
        collector = _collector;
        buffer = new HashMap<>();
        ClassBuffer = new HashMap<>();
        //f
    }
    @Override
    public void execute(Tuple tuple){
        opencv_core.IplImage fkImpage = new opencv_core.IplImage();
        Pair<String,Integer> key = new Pair<>(tuple.getStringByField("Filename"), tuple.getIntegerByField("Pack"));
        collector.ack(tuple);
        if(tuple.contains("Class")){
            if(!ClassBuffer.containsKey(key)) {
                ClassBuffer.put(key,tuple.getDoubleByField("Class"));
                long c = Math.round(Math.rint(ClassBuffer.get(key)));
                Iterator <Map.Entry<Integer,opencv_core.Mat>> x = buffer.get(key).entrySet().iterator();
                opencv_core.Mat timg;
                for(;x.hasNext();){
                    Map.Entry<Integer,opencv_core.Mat> tt = x.next();
                    timg = tt.getValue().clone();
                    timg = DrawOnCorner(timg, c, 3).clone();
                    collector.emit(new Values(new Serializable.Mat(timg),tuple.getStringByField("Filename"),tuple.getIntegerByField("Pack"),new Integer(tt.getKey()),tuple.getIntegerByField("Patch"),tuple.getIntegerByField("Scale"),tuple.getIntegerByField("sPatch")));
                }
                buffer.remove(key);
            }
        }else if(tuple.contains("Image")){
            opencv_core.Mat img = ((tool.Serializable.Mat)tuple.getValueByField("Image")).toJavaCVMat(),timg = new opencv_core.Mat();
            if(!ClassBuffer.containsKey(key)) {
                if (!buffer.containsKey(key)) {
                    buffer.put(key, new TreeMap<>());
                }
                buffer.get(key).put(tuple.getIntegerByField("Frame"), img);
            }else{
                long x = Math.round(Math.rint(ClassBuffer.get(key)));
                timg = DrawOnCorner(img,x,3).clone();
                collector.emit(new Values(new Serializable.Mat(timg),tuple.getStringByField("Filename"),tuple.getIntegerByField("Pack"),tuple.getIntegerByField("Frame"),tuple.getIntegerByField("Patch"),tuple.getIntegerByField("Scale"),tuple.getIntegerByField("sPatch")));
            }
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("Image","Filename","Pack","Frame","Patch","Scale","sPatch"));
    }
    opencv_core.Mat DrawOnCorner(opencv_core.Mat img,long c,int at){
        opencv_core.Mat timg = img.clone();
        int x0,y0,x1,y1;
        if(at%2==0){
            x0 = 0;
            x1 = (img.cols()/3);
        }else{
            x0 = img.cols()-img.cols()/3;
            x1 = img.cols();
        }
        if(at<2){
            y0 = 0;
            y1 = (img.rows()/8);
        }else {
            y0 = img.rows() - img.cols() / 8;
            y1 = img.rows() ;
        }
        opencv_core.putText(timg,"Class : "+String.valueOf(c),new opencv_core.Point(x0+10,y0+20),opencv_core.CV_FONT_HERSHEY_PLAIN,1,new opencv_core.Scalar(opencv_core.CV_RGB(256,0,0)));
        return timg;
    }

}
