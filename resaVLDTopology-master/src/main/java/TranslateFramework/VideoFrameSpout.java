package TranslateFramework;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
/**
 * Created by swrrt on 23/9/2015.
 */
public class VideoFrameSpout extends BaseRichSpout{
    SpoutOutputCollector collector;
    opencv_highgui.VideoCapture capture;
    boolean video_is_over;
    String name;
    int npack,limit;
    int pack,frame,contain;
    opencv_core.Mat [] Last;
    public VideoFrameSpout(String _name,int _npack,int _limit){
        name = _name;
        npack = _npack;
        limit = _limit;
    }
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector _collector){
        opencv_core.IplImage fkImpage = new opencv_core.IplImage();
        collector = _collector;
        capture = new opencv_highgui.VideoCapture(name);
	System.out.println(" [  VIDEO SPOUT  ] "+capture+" |||| "+name);
        Last = new opencv_core.Mat[npack];
        pack = 0;
        frame = 0;
        contain = npack/2; /* Number of frames between two packs*/
        video_is_over = false;
    }
    @Override
    public void nextTuple(){
        try{Thread.sleep(200);}
        catch (Exception e){}
        opencv_core.Mat x = new opencv_core.Mat();
        if(video_is_over)return ;
        if(pack==0||frame>=contain){
            System.out.printf("WOWOWOWOWOW now is pack %d\n",pack);
            if(!capture.read(x)){video_is_over = true;System.out.println(" !!!!!!!!!! OVER !!!!!!!!\n");return ;}
            if(frame>=contain){
                Last[frame - contain] = x.clone();
            }
        }else{
            x = Last[frame].clone();
        }
        collector.emit(new Values(new tool.Serializable.Mat(x),name,pack,frame,0,0,0));
        frame++;
        if(frame>=npack){
            frame=0;
            pack++;
        }
        if(pack>=limit){video_is_over = true;}
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("Image","Filename","Pack","Frame","Patch","Scale","sPatch"));
    }
}
