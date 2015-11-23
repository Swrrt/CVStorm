package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import logodetection.Util;
import org.bytedeco.javacpp.opencv_core;
import tool.Serializable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static tool.Constants.*;
import static topology.StormConfigManager.getString;

/**
 * Created by Tom Fu at Mar 24, 2015
 * This bolt is designed to draw found rects on each frame,
 * TODO: leave implementation for multiple target logos (currently one support one logo)
 */
public class tDrawPatchBolt extends BaseRichBolt {
    OutputCollector collector;

    //int lim = 31685; // SONY
    private HashMap<Integer, List<Serializable.Rect>> foundRectList;
    private HashMap<Integer, Serializable.Mat> frameMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;

        frameMap = new HashMap<>();
        foundRectList = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        opencv_core.IplImage imageFK = new opencv_core.IplImage();

        if (streamId.equals(PROCESSED_FRAME_STREAM)) {
            List<Serializable.Rect> list = (List<Serializable.Rect>) tuple.getValueByField(FIELD_FOUND_RECT_LIST);
            foundRectList.put(frameId, list);
            //System.out.println("PROCESSED_FRAME_STREAM: " + System.currentTimeMillis() + ":" + frameId);
        } else if (streamId.equals(RAW_FRAME_STREAM)) {
            Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
            frameMap.put(frameId, sMat);
            //System.out.println("RAW_FRAME_STREAM: " + System.currentTimeMillis() + ":" + frameId);
        }

        if (frameMap.containsKey(frameId) && foundRectList.containsKey(frameId)) {
            opencv_core.Mat mat = frameMap.get(frameId).toJavaCVMat();
            List<Serializable.Rect> list = foundRectList.get(frameId);

            if (list != null) {
                for (Serializable.Rect rect : list) {
                    Util.drawRectOnMat(rect.toJavaCVRect(), mat, opencv_core.CvScalar.MAGENTA);
                }
            }

            Serializable.Mat sMatNew = new Serializable.Mat(mat);
            collector.emit(STREAM_FRAME_DISPLAY, tuple, new Values(frameId, sMatNew));

            System.out.println("FrameDisplay-finishedAdd: " + frameId +"@" + System.currentTimeMillis());
            foundRectList.remove(frameId);
            frameMap.remove(frameId);
        } else {
            //System.out.println("tDrawPatchBetaFinished: " + System.currentTimeMillis() + ":" + frameId);
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_FRAME_DISPLAY, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}
