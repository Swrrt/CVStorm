package TranslateFramework;



import backtype.storm.task.OutputCollector;

import backtype.storm.task.TopologyContext;

import backtype.storm.topology.OutputFieldsDeclarer;

import backtype.storm.topology.base.BaseRichBolt;

import backtype.storm.tuple.Fields;

import backtype.storm.tuple.Tuple;

import backtype.storm.tuple.Values;

import com.esotericsoftware.kryo.io.Output;



import java.util.Map;



/**

 * Created by Zhao Chen on 23/12/2015.

 */

public class ValveBolt extends BaseRichBolt{

    OutputCollector collector;

    int x,in,out;

    public ValveBolt(int in,int out){

        this.in = in;

        this.out = out;

    }

    @Override

    public void prepare(Map conf, TopologyContext context, OutputCollector collector){

        this.collector = collector;

        x = 0;

    }

    @Override

    public void execute(Tuple tuple){

        x++;

        if(x>=out){

            x=0;

            collector.emit("Control", new Values(in));

        }

    }

    @Override

    public void declareOutputFields(OutputFieldsDeclarer declarer){

        declarer.declare(new Fields("Number"));

    }

}
