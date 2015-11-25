package TranslateFramework;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import javafx.util.Pair;
import org.bytedeco.javacpp.DoublePointer;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacpp.opencv_imgproc;
import tool.Serializable;

import java.nio.FloatBuffer;
import java.util.*;

public class DenseTrajectBolt extends BaseRichBolt {
    int npack;
    int median_side;
    int traj_side;
    OutputCollector collector;
    Map<Pair<String,Pair<Integer,Pair<Integer,Pair<Integer,Integer>>>>,Pair<Map<Integer,Pair<Pair<Double[][],Double[][]>,opencv_core.Mat>>,Long>> buffer;  /* The Long has some potential problem */
    Map<Long,Pair<String,Pair<Integer,Pair<Integer,Pair<Integer,Integer>>>>> queue;
    long n;
    long Maxn;
    Values tmp;
    boolean isFirst;
    public DenseTrajectBolt(int _npack,int _median_side,int _traj_side){
        npack = _npack;
        median_side = _median_side;
        traj_side = _traj_side;
    }
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector _collector){
        opencv_core.IplImage fkImpage = new opencv_core.IplImage();
        collector = _collector;
        Maxn = 200000;
        buffer = new HashMap<>();
        queue = new TreeMap<>();
        isFirst = true;
        n=0;
    }
    @Override
    public void execute(Tuple tuple) {
        opencv_core.Mat img = ((tool.Serializable.Mat)tuple.getValueByField("Image")).toJavaCVMat();
        String filename = tuple.getStringByField("Filename");
        //System.out.println("DENSE is here!!!"+String.valueOf(n));
        int pack = tuple.getIntegerByField("Pack"),frame = tuple.getIntegerByField("Frame"), patch = tuple.getIntegerByField("Patch"), scale = tuple.getIntegerByField("Scale"), sPatch = tuple.getIntegerByField("sPatch");
        Double [][] Vx = (Double[][])tuple.getValueByField("OFx"),Vy = (Double[][])tuple.getValueByField("OFy");
        /* Buffer with LRU policy */
        if(buffer.containsKey(new Pair<>(filename,new Pair<>(pack,new Pair<>(patch,new Pair(scale,sPatch)))))){
            buffer.get(new Pair<>(filename,new Pair<>(pack,new Pair<>(patch,new Pair(scale,sPatch))))).getKey().put(frame, new Pair<>(new Pair<>(Vx, Vy), img));
            long x = buffer.get(new Pair<>(filename,new Pair<>(pack,new Pair<>(patch,new Pair(scale,sPatch))))).getValue();
            queue.remove(x);
            queue.put(n, new Pair<>(filename, new Pair<>(pack, new Pair<>(patch, new Pair(scale, sPatch)))));
            buffer.put(new Pair<>(filename,new Pair<>(pack,new Pair<>(patch,new Pair(scale,sPatch)))),new Pair<>(buffer.get(new Pair<>(filename,new Pair<>(pack,new Pair<>(patch,new Pair(scale,sPatch))))).getKey(),n));       /*  Holy Sh*t! */
            collector.ack(tuple);
        }else{
            if(!queue.entrySet().isEmpty()&&n-queue.entrySet().iterator().next().getKey()>Maxn){
                buffer.remove(queue.entrySet().iterator().next().getValue());
                queue.remove(queue.entrySet().iterator().next().getValue());
            }
            buffer.put(new Pair<>(filename,new Pair<>(pack,new Pair<>(patch,new Pair(scale,sPatch)))),new Pair<>(new TreeMap<>(),n));
            buffer.get(new Pair<>(filename,new Pair<>(pack,new Pair<>(patch,new Pair(scale,sPatch))))).getKey().put(frame, new Pair<>(new Pair<>(Vx, Vy), img));
            queue.put(n, new Pair<>(filename, new Pair<>(pack, new Pair<>(patch, new Pair(scale, sPatch)))));
            collector.ack(tuple);
        }
        //System.out.printf(" Dense:  %d pack, %d frame , %d\n",pack,frame,buffer.get(new Pair<>(filename,new Pair<>(pack,new Pair<>(patch,new Pair(scale,sPatch))))).getKey().size());
        n = n+1;
        if(buffer.get(new Pair<>(filename,new Pair<>(pack,new Pair<>(patch,new Pair(scale,sPatch))))).getKey().size() == npack) {
            System.out.println("YEYEYE!");
            Map<Integer, Pair<Pair<Double[][], Double[][]>, opencv_core.Mat>> bb = buffer.get(new Pair<>(filename, new Pair<>(pack, new Pair<>(patch, new Pair(scale, sPatch))))).getKey();
            opencv_core.Mat[] x = new opencv_core.Mat[npack];
            List<List<Double>> px = new ArrayList<>(), py = new ArrayList<>();
            List<Double> tpx = new ArrayList<>(), tpy = new ArrayList<>();
            opencv_core.Mat eigen = new opencv_core.Mat();
            opencv_core.Mat gimg = new opencv_core.Mat();
            x[0] = bb.get(0).getValue().clone();
            opencv_imgproc.cvtColor(x[0], gimg, opencv_imgproc.CV_BGR2GRAY);
            opencv_imgproc.cornerMinEigenVal(gimg, eigen, 5);
            double[] max=new double[1];
            opencv_core.minMaxLoc(eigen, null, max, null, new opencv_core.Point(),new opencv_core.Mat());
            double e_thres = max[0]*0.0001;
            int bound = 15;
            for (int i = bound; i < x[0].rows() - bound; i++)
                for (int j = bound; j < x[0].cols() - bound; j++) {
                    if (i % 5 == 3 && j % 5 == 3) {
                        double v = eigen.ptr(i,j).get();
                        if(v>e_thres) {
                            tpx.add((double) j);
                            tpy.add((double) i);
                        }
                    }

                }
            px.add(tpx);
            py.add(tpy);

            List<tool.Serializable.Mat> images = new ArrayList<>();
            tmp = new Values();
            isFirst = true;
            List<Double[][]> OFxs = new ArrayList<>(), OFys = new ArrayList<>();
            List<Double> Xs = new ArrayList<>(),Ys =new ArrayList<>();
	    
            for(int j=0;j<npack;j++)x[j]= bb.get(j).getValue();
            for(int i=0;i<tpx.size();i++){
                //System.out.printf("SOMETHING???? %d\n",i);
                images = new ArrayList<>();
                OFxs = new ArrayList<>();
                OFys = new ArrayList<>();
                Xs = new ArrayList<>();
                Ys = new ArrayList<>();
                double txx = tpx.get(i),tyy = tpy.get(i);
                //int tx = (int)Math.round(tpx.get(i)), ty = (int)Math.round(tpy.get(i));
                for(int j=0;j<npack;j++){
                    opencv_core.Mat timg = bb.get(j).getValue();
                    Vx = bb.get(j).getKey().getKey();
                    Vy = bb.get(j).getKey().getValue();
                    double dx = 0, dy = 0;
                    int sx=0,sy=0;
                    for(int xx = (int)txx - median_side;xx <= (int)txx+median_side ;xx++)
                        for(int yy = (int)tyy - median_side;yy <= (int)tyy+median_side ;yy++)
                            if (xx >= 0 && xx < x[0].cols() && yy >= 0 && yy < x[0].rows() && Vx[yy][xx] * Vx[yy][xx] + Vy[yy][xx] * Vy[yy][xx] < 100) {
                                dx+=Vx[yy][xx];
                                dy+=Vy[yy][xx];
                                sx++;
                                sy++;
                            }
                    dx/=sx;
                    dy/=sy;
/*                    if((int)(txx+dx)>=traj_side&&(int)(txx+dx)<timg.cols()-traj_side&&(int)(tyy+dy)>=traj_side&&(int)(tyy+dy)<timg.rows()-traj_side){
                        for(int k=j;k<npack;k++){
                            opencv_core.line(x[k], new opencv_core.Point((int) (txx), (int) (tyy)), new opencv_core.Point((int) (txx+dx), (int) (tyy+dy)), new opencv_core.Scalar(opencv_core.CV_RGB(255 * (14 + j - k) / 14, 0, 0)));
                        }
                    }*/
                    txx += dx;
                    tyy += dy;
                    if(txx>=traj_side&&txx<timg.cols()-traj_side&&tyy>=traj_side&&tyy<timg.rows()-traj_side){
                        Double[][] ofx = new Double[traj_side*2+1][traj_side*2+1], ofy = new Double[traj_side*2+1][traj_side*2+1];
                        opencv_core.Mat ttimg = new opencv_core.Mat();
                        ttimg = timg.apply(new opencv_core.Rect((int)txx - traj_side, (int)tyy - traj_side, traj_side * 2+1, traj_side * 2+1)).clone();
                        for(int xx = (int)txx - traj_side; xx<= (int) txx +traj_side; xx++){
                            for(int yy = (int)tyy - traj_side; yy<= (int) tyy +traj_side; yy++){
                                ofx[yy-(int)tyy+traj_side][xx-(int)txx+traj_side] = Vx[yy][xx];
                                ofy[yy-(int)tyy+traj_side][xx-(int)txx+traj_side] = Vy[yy][xx];
                            }
                        }
                        images.add(new tool.Serializable.Mat(ttimg));
                        Xs.add(dx);
                        Ys.add(dy);
                        OFxs.add(ofx);
                        OFys.add(ofy);
                    }
                }
                if(OFxs.size()==npack) {
                    if (!isFirst) {
                        collector.emit(tmp);
                    }
                    isFirst = false;
                    tmp = new Values(images, tuple.getStringByField("Filename"), tuple.getIntegerByField("Pack"), 0, tuple.getIntegerByField("Patch"), tuple.getIntegerByField("Scale"), i, OFxs, OFys, Xs,Ys, false);
                }
            }
            if(!isFirst){
                tmp.set(tmp.size()-1,true);
                collector.emit(tmp);
            }
/*            for(int j=0;j<npack;j++){
                String name = tuple.getStringByField("Filename");
                int pp = name.length()-1;
                while(pp>=0&&name.charAt(pp)!='\\')pp--;
                System.out.println(name.substring(0, pp) + "\\output\\" + name.substring(pp + 1, name.length() - 4) + "_" + String.valueOf(tuple.getIntegerByField("Pack")) + "_" + String.valueOf(tuple.getIntegerByField("Patch")) + "_" + String.valueOf(tuple.getIntegerByField("Scale"))+"_"+String.valueOf(j) + ".jpg");
                opencv_highgui.imwrite(name.substring(0, pp) + "\\output\\" + name.substring(pp + 1, name.length() - 4) + "_" + String.valueOf(tuple.getIntegerByField("Pack")) + "_" + String.valueOf(tuple.getIntegerByField("Patch")) + "_" + String.valueOf(tuple.getIntegerByField("Scale"))+"_"+String.valueOf(j) + ".jpg",x[j]);
            }*/
            /*for (int i = 0; i < npack; i++) {
                //Double [][] Vx = new Double[x[0].rows()][x[0].cols()],Vy = new Double[x[0].rows()][x[0].cols()];
                x[i] = bb.get(i).getValue();
                Vx = bb.get(i).getKey().getKey();
                Vy = bb.get(i).getKey().getValue();
                bb.remove(i);
                //System.out.println(x[i].rows());
                opencv_core.Mat line = x[i].clone();
                tpx = new ArrayList<>();
                tpy = new ArrayList<>();
                for (int j = 0; j < px.get(i).size(); j++) {
                    int tx = (int) px.get(i).get(j).doubleValue(), ty = (int) py.get(i).get(j).doubleValue();
                    if (tx >= 0 && tx < x[0].cols() && ty >= 0 && ty < x[0].rows()) {
                        List<Double> t1 = new ArrayList<>();
                        List<Double> t2 = new ArrayList<>();
                        double s1 = 0, s2 = 0;
                        for (int xx = tx - median_side; xx <= tx + median_side; xx++)
                            for (int yy = ty - median_side; yy <= ty + median_side; yy++)
                                if (xx >= 0 && xx < x[0].cols() && yy >= 0 && yy < x[0].rows() && Vx[yy][xx] * Vx[yy][xx] + Vy[yy][xx] * Vy[yy][xx] < 100) {
                                    t1.add(Vx[yy][xx]);
                                    s1 += Vx[yy][xx];
                                    t2.add(Vy[yy][xx]);
                                    s2 += Vy[yy][xx];
                                }
                        Collections.sort(t1);
                        Collections.sort(t2);
                        double dx = s1 / (t1.size()), dy = s2 / (t2.size());
                        if (dx * dx + dy * dy < 1000) {
                            for (int ti = 0; ti < i; ti++) {
                                opencv_core.line(line, new opencv_core.Point((int) (px.get(ti).get(j).doubleValue()), (int) (py.get(ti).get(j).doubleValue())), new opencv_core.Point((int) (px.get(ti + 1).get(j).doubleValue()), (int) (py.get(ti + 1).get(j).doubleValue())), new opencv_core.Scalar(opencv_core.CV_RGB(255 * (14 + ti - i) / 14, 0, 0)));
                            }
                            opencv_core.line(line, new opencv_core.Point(tx, ty), new opencv_core.Point((int) (px.get(i).get(j).doubleValue() - dx), (int) (py.get(i).get(j).doubleValue() - dy)), new opencv_core.Scalar(opencv_core.CV_RGB(255, 0, 0)));
                            if(tx>=traj_side&&tx+traj_side<x[0].cols()&&ty>=traj_side&&ty+traj_side<x[0].rows()) {
                                opencv_core.Mat spx = new opencv_core.Mat();
                                spx = x[i].apply(new opencv_core.Rect(tx - traj_side, ty - traj_side, traj_side * 2+1, traj_side * 2+1)).clone();
                                Double[][] OFx = new Double[traj_side * 2 + 1][traj_side * 2 + 1], OFy = new Double[traj_side * 2 + 1][traj_side * 2 + 1];
                                for(int ttx=0;ttx<traj_side*2+1;ttx++)
                                    for(int tty=0;tty<traj_side*2+1;tty++){
                                        OFx[tty][ttx] = Vx[tty+ty-traj_side][ttx+tx-traj_side];
                                        OFy[tty][ttx] = Vy[tty+ty-traj_side][ttx+tx-traj_side];
                                    }
                                try{
                                    Thread.sleep(100);
                                }catch (Exception e){};

                                //collector.emit(new Values(new tool.Serializable.Mat(spx), tuple.getStringByField("Filename"), tuple.getIntegerByField("Pack"), i, tuple.getIntegerByField("Patch"), tuple.getIntegerByField("Scale"), j,OFx,OFy));

                            }
                            tpx.add(px.get(i).get(j).doubleValue() - dx);
                            tpy.add(py.get(i).get(j).doubleValue() - dy);
                        } else {
                            tpx.add(px.get(i).get(j).doubleValue());
                            tpy.add(py.get(i).get(j).doubleValue());
                        }
                    } else {
                        tpx.add(px.get(i).get(j).doubleValue());
                        tpy.add(py.get(i).get(j).doubleValue());
                    }
                }
                px.add(tpx);
                py.add(tpy);
                String name = tuple.getStringByField("Filename");
                int pp = name.length()-1;
                while(pp>=0&&name.charAt(pp)!='\\')pp--;
                System.out.println(name.substring(0,pp) + "\\output\\" + name.substring(pp+1,name.length()-4) + "_" + String.valueOf(tuple.getIntegerByField("Pack")) + "_" + String.valueOf(tuple.getIntegerByField("Patch")) + "_" + String.valueOf(i) + ".jpg");
                //opencv_highgui.imwrite(name.substring(0, pp) + "\\output\\" + name.substring(pp + 1, name.length() - 4) + "_" + String.valueOf(tuple.getIntegerByField("Pack")) + "_" + String.valueOf(tuple.getIntegerByField("Patch")) + "_" + String.valueOf(i) + ".jpg",line);
            }*/

            queue.remove(buffer.get(new Pair<>(filename, new Pair<>(pack, new Pair<>(patch, new Pair(scale, sPatch))))).getValue());
            buffer.remove(new Pair<>(filename, new Pair<>(pack, new Pair<>(patch, new Pair(scale, sPatch)))));

        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("Images","Filename","Pack","Frame","Patch","Scale","sPatch","OFxs","OFys","Xs","Ys","Last"));
    }
}
