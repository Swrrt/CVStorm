package TranslateFramework;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;

import java.awt.*;
import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by swrrt on 23/9/2015.
 */
public class ImageReadSpout extends BaseRichSpout {
    SpoutOutputCollector collector;
    File [] files;
    int x;
    String path;
    public ImageReadSpout(String _path){
        path = _path;
    }
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector _collector){
        opencv_core.IplImage fkImpage = new opencv_core.IplImage();
        collector = _collector;

        files = (new File(path)).listFiles();
        x=0;
    }
    @Override
    public void nextTuple(){
        if(x<files.length){
            if(files[x].exists()){
                opencv_core.Mat img = opencv_highgui.imread(path+files[x].getName());
		String tname = files[x].getName();
                int pack=0,frame=0,patch=0,scale=0,spatch=0,len = tname.length();
		/* For different image filename module */
                if(tname.contains("PACK_")){
                    int i=tname.indexOf("PACK_")+5;
                    int j=i;
                    while(tname.charAt(i)>='0'&&tname.charAt(i)<='9')i++;
                    pack = Integer.parseInt(tname.substring(j,i));
                    tname = tname.substring(0,j-5)+tname.substring(i);
                }
                if(tname.contains("FRAME_")){
                    int i=tname.indexOf("FRAME_")+6;
                    int j=i;
                    while(tname.charAt(i)>='0'&&tname.charAt(i)<='9')i++;
                    frame = Integer.parseInt(tname.substring(j,i));
                    tname = tname.substring(0,j-6)+tname.substring(i);
                }
		/* new */
		if(len>0){
		    int i = len - 5;
                    int j = i+1;
		    while(i>=0&&tname.charAt(i)>='0'&&tname.charAt(i)<='9')i--;
		    if(i>=5&&tname.charAt(i)=='_'){
			spatch = Integer.parseInt(tname.substring(i+1,j));
			j=i;
			i--;
			while(i>=0&&tname.charAt(i)>='0'&&tname.charAt(i)<='9')i--;
			scale = Integer.parseInt(tname.substring(i+1,j));
			j=i;
			i--;
			while(i>=0&&tname.charAt(i)>='0'&&tname.charAt(i)<='9')i--;
			patch = Integer.parseInt(tname.substring(i+1,j));
			j=i;
			i--;
			while(i>=0&&tname.charAt(i)>='0'&&tname.charAt(i)<='9')i--;
			frame = Integer.parseInt(tname.substring(i+1,j));
			j=i;
			i--;
			while(i>=0&&tname.charAt(i)>='0'&&tname.charAt(i)<='9')i--;
			pack = Integer.parseInt(tname.substring(i+1,j));
		    }
		    
		}
                collector.emit(new Values(new tool.Serializable.Mat(img), path+tname, pack, frame, patch, scale, spatch));
		x++;
            }
        }
	if(x%90==0)try{Thread.sleep(6000);}catch(Exception e){}
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){
        outputFieldsDeclarer.declare(new Fields("Image","Filename","Pack","Frame","Patch","Scale","sPatch"));
    }
}
