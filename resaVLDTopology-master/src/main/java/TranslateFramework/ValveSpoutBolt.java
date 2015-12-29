package TranslateFramework;



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

        x=0;

    }

    @Override

    public void execute(Tuple tuple){

        int p = tuple.getIntegerByField("Number");

        while(p>0){

            if(x<files.length){

                if(files[x].exists()){

                    opencv_core.Mat img = opencv_highgui.imread(path + files[x].getName());

                    collector.emit(new Values(new tool.Serializable.Mat(img), files[x].getName(), 0, 0, 0, 0, 0));

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
