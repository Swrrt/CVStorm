package TranslateFramework;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.bytedeco.javacpp.opencv_core;
import redis.clients.jedis.Jedis;
import tool.RedisStreamProducerFox;
import tool.Serializable;
import topology.StreamFrame;

import java.util.Map;

/**
 * Created by Zhao Chen on 9/11/2015.
 */
public class RedisOutputBolt extends BaseRichBolt {
    OutputCollector collector;
    RedisStreamProducerFox producer;
    String host,queueName;
    int port;
    int npack;
    public RedisOutputBolt(String _host, int _port, String _queueName,int _npack){
        host = _host;
        port = _port;
        queueName = _queueName;
        npack = _npack;
    }
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector _collector){
        collector = _collector;
        producer = new RedisStreamProducerFox(host,port,queueName,0,200,10);
        new Thread(producer).start();
    }
    @Override
    public void execute(Tuple tuple){
        int Pack,Frame,frameId;
        Pack = tuple.getIntegerByField("Pack");
        Frame = tuple.getIntegerByField("Frame");
        opencv_core.Mat img = ((tool.Serializable.Mat)tuple.getValueByField("Image")).toJavaCVMat();
        frameId = Pack*npack+Frame;
        producer.addFrame(new StreamFrame(frameId,img));
        collector.ack(tuple);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
    }
}
