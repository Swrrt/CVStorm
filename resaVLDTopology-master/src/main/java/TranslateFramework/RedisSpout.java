package TranslateFramework;

import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
import redis.clients.jedis.Jedis;
import tool.RedisQueueSpout;
import tool.Serializable;

import java.util.Map;

/**
 * Created by Zhao Chen on 5/11/2015.
 */
public class RedisSpout extends RedisQueueSpout {
    SpoutOutputCollector collector;
    int frameId;
    int npack;
    String name;
    public RedisSpout(String host,int port,String queue,int npack){
        super(host,port,queue,true);
        name = queue;
System.out.println(" [ Redis Spout ] "+name);
        opencv_core.IplImage fkImage = new opencv_core.IplImage();
        this.npack = npack;
    }
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector _collector){
        super.open(conf, context, _collector);
        collector = _collector;
        frameId = 0;

    }
    @Override
    public void emitData(Object data){
        int pack = frameId/npack, frame = frameId%npack;
        byte[] imgData = (byte[])data;
        Serializable.Mat img = new Serializable.Mat(imgData);
        collector.emit(new Values(img,name,pack,frame,0,0,0));
        frameId++;
	if(frameId==30){
	try{
		Thread.sleep(6000);
	}catch(Exception e){}
	}else if(frameId%90==0){
	try{
		Thread.sleep(15000);
	}catch(Exception e){}
	}
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("Image","Filename","Pack","Frame","Patch","Scale","sPatch"));
    }
}
