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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Zhao Chen on 11/11/2015.
 */
public class OpticalFlowBolt_latent extends BaseRichBolt {
    OutputCollector collector;
    Map<Pair<String,Pair<Integer,Pair<Integer,Pair<Integer,Pair<Integer,Integer>>>>>,Pair<opencv_core.Mat,Long>> bufferl,buffern;  /* The Long has some potential problem */
    Map<Long,Pair<String,Pair<Integer,Pair<Integer,Pair<Integer,Pair<Integer,Integer>>>>>> queuel,queuen;
    long n,Maxn;
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector){
        this.collector = collector;
        n=0;
        Maxn = 20000;
        buffern = new HashMap<>();
        bufferl = new HashMap<>();
        queuel = new TreeMap<>();
        queuen = new TreeMap<>();
    }
    @Override
    public void execute(Tuple tuple){
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
            collector.emit(new Values(new tool.Serializable.Mat(y),new tool.Serializable.Mat(x),filename,pack,frame-1,patch,scale,sPatch));
            //System.out.printf("OF out put: pack %d frame %d\n", pack, frame - 1);
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
            collector.emit(new Values(new tool.Serializable.Mat(x),new tool.Serializable.Mat(y), filename, pack, frame, patch, scale, sPatch));
            //System.out.printf("OF out put: pack %d frame %d\n", pack, frame);
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
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("Image0","Image1","Filename","Pack","Frame","Patch","Scale","sPatch"));
    }
}
