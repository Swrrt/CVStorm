package TranslateFramework;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.FileWriter;
import java.util.List;
import java.util.Map;
import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacpp.opencv_core;
/**
 * Created by swrrt on 5/10/2015.
 */
public class OutputToFileBolt extends BaseRichBolt {
    OutputCollector collector;
    String path;
    public OutputToFileBolt(String _path){
        path = _path;
    }
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector _collector){
        collector = _collector;
    }
    @Override
    public void execute(Tuple tuple){
        opencv_core.IplImage fkImpage = new opencv_core.IplImage();
        collector.ack(tuple);
        String name = tuple.getStringByField("Filename");
        int i=name.length()-1;
        while(i>=0&&name.charAt(i)!='/')i--;
        if(i>=0){
            name = name.substring(i+1,name.length());
        }
        if(tuple.contains("F_type"))name = name + tuple.getStringByField("F_type");
        name = name + String.valueOf(tuple.getIntegerByField("Pack")) + "_" + String.valueOf(tuple.getIntegerByField("Frame"))+ "_"
                + String.valueOf(tuple.getIntegerByField("Patch")) + "_"+String.valueOf(tuple.getIntegerByField("Scale"))+"_"+String.valueOf(tuple.getIntegerByField("sPatch")) + ".txt";
        System.out.println("[ Output ] "+path+name);
        if(tuple.contains("Image")) {
            opencv_highgui.imwrite(path+name.substring(0,name.length()-4)+".jpg",((tool.Serializable.Mat)tuple.getValueByField("Image")).toJavaCVMat());
        }else {
            try {
                FileWriter writer = new FileWriter(path + name, false);
                System.out.println("zzzzzzzzzzzzzz " + tuple.getFields().get(0));
                if (tuple.contains("Feature")) {
                    List<Double> f = (List<Double>) tuple.getValueByField("Feature");
                    writer.write(String.valueOf(f.size()) + "\n");
                    for (int j = 0; j < f.size(); j++) {
                        writer.write(String.valueOf(f.get(j)) + " ");
                    }
                } else if (tuple.contains("Features")) {
                    List<List<Double>> f = (List<List<Double>>) tuple.getValueByField("Features");
                    writer.write(String.valueOf(f.size()) + "\n");
                    //System.out.println(String.valueOf(f.size())+"\n");
                    for (int j = 0; j < f.size(); j++) {
                        writer.write(String.valueOf(f.get(j).size()) + "\n");
                        for (int k = 0; k < f.get(j).size(); k++)
                            writer.write(String.valueOf(f.get(j).get(k)) + " ");
                        writer.write("\n");
                    }
                } else {
                    writer.write(tuple.getFields().get(0));
                }
                writer.flush();
                writer.close();
            } catch (Exception e) {
		System.out.println(e);
            }
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){}
}
