package TranslateFramework;



import backtype.storm.spout.SpoutOutputCollector;

import backtype.storm.task.TopologyContext;

import backtype.storm.topology.OutputFieldsDeclarer;

import backtype.storm.topology.base.BaseRichSpout;

import backtype.storm.tuple.Fields;

import backtype.storm.tuple.Values;



import java.util.Map;



/**

 * Created by Zhao Chen on 29/12/2015.

 */

public class ValveVirtualSpout extends BaseRichSpout {

    int t,n;

    SpoutOutputCollector collector;

    public ValveVirtualSpout(int timeout,int number){

        t = timeout;

        n = number;

    }

    @Override

    public void open(Map conf, TopologyContext context, SpoutOutputCollector _collector){

        collector = _collector;

    }

    @Override

    public void nextTuple(){

        do{

            collector.emit(new Values(n));

            try{

                Thread.sleep(t*1000);

            }catch (Exception e){}

        }while(true);

    }

    @Override

    public void declareOutputFields(OutputFieldsDeclarer declarer){

        declarer.declare(new Fields("Number"));

    }

}
