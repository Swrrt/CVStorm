package TranslateFramework;


import java.util.Collections;
import java.util.Arrays;
import java.util.Comparator;
import backtype.storm.task.OutputCollector;

import backtype.storm.task.TopologyContext;

import backtype.storm.topology.OutputFieldsDeclarer;

import backtype.storm.topology.base.BaseRichBolt;

import backtype.storm.tuple.Fields;

import backtype.storm.tuple.Tuple;

import backtype.storm.tuple.Values;

import org.bytedeco.javacpp.opencv_core;

import org.bytedeco.javacpp.opencv_highgui;



import java.io.File;

import java.util.Map;



/**

 * Created by Zhao Chen on 28/12/2015.

 */

public class ValveSpoutBolt extends BaseRichBolt {

    OutputCollector collector;

    File [] files;

    int x;

    String path;

    public ValveSpoutBolt(String path){

        this.path = path;

    }

    @Override

    public void prepare(Map conf, TopologyContext context, OutputCollector collector){

        this.collector = collector;

        opencv_core.IplImage fkImpage = new opencv_core.IplImage();
        files = (new File(path)).listFiles();
	Collections.sort(Arrays.asList(files),new Comparator<File>(){
		@Override
		public int compare(File f1, File f2){
			return f1.getName().compareTo(f2.getName());
		}
	});
        x=0;
    }
    @Override

    public void execute(Tuple tuple){

        int p = tuple.getIntegerByField("Number");

        while(p>0){
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
		    if(i>=3&&tname.charAt(i)=='_'){
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
			tname = tname.substring(0,i+1);
		    }
		    
		}
                collector.emit(new Values(new tool.Serializable.Mat(img), path+tname, pack, frame, 0, 0, 0));
           }
      }
            x++;
            p--;

        }

    }

    @Override

    public void declareOutputFields(OutputFieldsDeclarer declarer){

        declarer.declare(new Fields("Image","Filename","Pack","Frame","Patch","Scale","sPatch"));

    }

}
