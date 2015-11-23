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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by swrrt on 22/9/2015.
 */
public class DevideImageBolt extends BaseRichBolt{
    OutputCollector collector;
    double rate;
    String name;
    public DevideImageBolt(double _rate){
        rate = _rate;
    }
    @Override
    public void prepare(Map conf, TopologyContext context,OutputCollector _collector){
        collector = _collector;
        name = context.getThisComponentId();

    }
    @Override
    public void execute(Tuple tuple){
        opencv_core.IplImage fkImpage = new opencv_core.IplImage();
        opencv_core.Mat img;
        img =((tool.Serializable.Mat)tuple.getValueByField("Image")).toJavaCVMat();
        Img_cut(img,tuple);
    }
    public void Img_cut(opencv_core.Mat img, Tuple tuple){
        int c=0;
        int row = (int)Math.ceil(img.rows()/rate);
        int col = (int)Math.ceil(img.cols() / rate);

        opencv_core.Mat timg = new opencv_core.Mat();
        /*  cut image into patches with overlap */
        for(int i=0;i+row<=img.rows();i+=row/2)
            for(int j=0;j+col<=img.cols();j+=col/2){
                timg = img.apply(new opencv_core.Rect(j,i,col,row)).clone();
                collector.emit(new Values(new tool.Serializable.Mat(timg), tuple.getStringByField("Filename"), tuple.getIntegerByField("Pack"), tuple.getIntegerByField("Frame"), c, 0, 0));
                c++;
            }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("Image","Filename","Pack","Frame","Patch","Scale","sPatch"));
    }


}
