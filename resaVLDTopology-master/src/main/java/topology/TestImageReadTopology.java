package topology;

import TranslateFramework.DevideImageBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacpp.opencv_imgproc;


/**
 * Created by swrrt on 24/8/2015.
 */
public class TestImageReadTopology {
    /*public static class TestImageReadSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;
        Set<String> files;
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
            _collector = collector;
            files = new HashSet<String>();
        }
        @Override
        public void nextTuple(){
            File[] tfiles = (new File("C:/testing")).listFiles();
            opencv_core.IplImage fkImpage = new opencv_core.IplImage();
            for(File f:tfiles){
                if(f.isFile() && !files.contains(f.getName())){
                    files.add(f.getName());
                    opencv_core.Mat image = opencv_highgui.imread("C:/testing/"+f.getName(),1);
                    _collector.emit(new Values(new tool.Serializable.Mat(image),f.getName()));
                }
            }
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("Image","Name"));
        }
    }*/
    public static class FilterBolt extends BaseBasicBolt{
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector){
            opencv_core.IplImage fkImpage = new opencv_core.IplImage();
            opencv_core.Mat x = ((tool.Serializable.Mat)(tuple.getValue(1))).toJavaCVMat(), y = new opencv_core.Mat();
            opencv_core.Mat x1 = ((tool.Serializable.Mat)(tuple.getValue(2))).toJavaCVMat(), y1 = new opencv_core.Mat();
            opencv_imgproc.medianBlur(x, y, 1);
            opencv_imgproc.medianBlur(x1, y1, 1);
            collector.emit(new Values(tuple.getInteger(0).intValue(),new tool.Serializable.Mat(y), new tool.Serializable.Mat(y1)));
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){
            outputFieldsDeclarer.declare(new Fields("ID","First","Last"));
        }
    }
    public static class VideoDoubleFrameSpout extends BaseRichSpout{
        SpoutOutputCollector _collector;
        opencv_highgui.VideoCapture capture;
        boolean video_is_over;
        int ID;
        opencv_core.Mat Last;
        @Override
        public void open(Map conf,TopologyContext context,SpoutOutputCollector collector){
            opencv_core.IplImage fkImpage = new opencv_core.IplImage();
            _collector = collector;
            video_is_over = false;
            ID = 0;
            Last = new opencv_core.Mat();
            System.out.println("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");

            do{
                capture = new opencv_highgui.VideoCapture("C:/testing/1.avi");
                System.out.println("????\n");
            }while(!capture.isOpened());
            if(capture.read(Last));else video_is_over = true;
            opencv_imgproc.resize(Last,Last,new opencv_core.Size(Last.cols()/2,Last.rows()/2));
        }
        @Override
        public void nextTuple(){
            try{
                Thread.sleep(100);
            }catch(Exception e){
            }
            if(!video_is_over&&capture.isOpened()){
                opencv_core.Mat a = new opencv_core.Mat(Last),b = new opencv_core.Mat();
                if(capture.read(b)) {
                    opencv_imgproc.resize(b,b,new opencv_core.Size(b.cols()/2,b.rows()/2));
                    ID++;
                    _collector.emit(new Values(ID, new tool.Serializable.Mat(a), new tool.Serializable.Mat(b)));


                }else{
                     video_is_over = true;
                }
                Last = b;
            }
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){
            outputFieldsDeclarer.declare(new Fields("ID", "First", "Last"));
        }

    }
    public static class Video15FrameSpout extends BaseRichSpout{
        SpoutOutputCollector _collector;
        opencv_highgui.VideoCapture capture;
        boolean video_is_over;
        int ID;
        opencv_core.Mat[] a;
        @Override
        public void open(Map conf,TopologyContext context,SpoutOutputCollector collector){
            opencv_core.IplImage fkImpage = new opencv_core.IplImage();
            _collector = collector;
            video_is_over = false;
            ID = 0;
            a = new opencv_core.Mat[15];

            System.out.println("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");

            do{
                capture = new opencv_highgui.VideoCapture("C:/testing/1.avi");
                System.out.println("????\n");
            }while(!capture.isOpened());
            a[14] = new opencv_core.Mat();
            for(int i=0;i<14;i++) {
                a[i] = new opencv_core.Mat();
                /*if (capture.read(a[i])) ;
                else {
                    video_is_over = true;
                    break;
                }*/
            }
        }
        @Override
        public void nextTuple(){
            try{
                Thread.sleep(100);
            }catch(Exception e){
            }
            if(!video_is_over&&capture.isOpened()){
                boolean fl = true;
                for(int i=0;i<15;i++){
                    fl &= capture.read(a[i]);
                    //opencv_imgproc.resize(a[i],a[i],new opencv_core.Size(a[i].cols()/4,a[i].rows()/4));
                }
                if(fl&&ID<100) {
                    ID++;
                    _collector.emit(new Values(ID, new tool.Serializable.Mat(a[0]),new tool.Serializable.Mat(a[1]),new tool.Serializable.Mat(a[2]),new tool.Serializable.Mat(a[3]),new tool.Serializable.Mat(a[4]),
                    new tool.Serializable.Mat(a[5]),new tool.Serializable.Mat(a[6]),new tool.Serializable.Mat(a[7]),new tool.Serializable.Mat(a[8]),new tool.Serializable.Mat(a[9]),
                    new tool.Serializable.Mat(a[10]),new tool.Serializable.Mat(a[11]),new tool.Serializable.Mat(a[12]),new tool.Serializable.Mat(a[13]),new tool.Serializable.Mat(a[14])
                    ));
                    for(int i=0;i<14;i++)a[i]=a[i+1].clone();
                    //video_is_over = true;
                }else{
                    video_is_over = true;
                }
            }
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){
            outputFieldsDeclarer.declare(new Fields("ID", "Image1", "Image2", "Image3", "Image4", "Image5", "Image6", "Image7", "Image8", "Image9", "Image10", "Image11", "Image12", "Image13", "Image14", "Image15"));
        }

    }
    public static class OpticalFlowBolt extends BaseBasicBolt{
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector){
            opencv_core.IplImage fkImpage = new opencv_core.IplImage();
            opencv_core.Mat ximg = ((tool.Serializable.Mat)tuple.getValueByField("First")).toJavaCVMat(),yimg = ((tool.Serializable.Mat)tuple.getValueByField("Last")).toJavaCVMat(),z = new opencv_core.Mat(),z1 = new opencv_core.Mat(), z2 = new opencv_core.Mat();
            //opencv_video.calcOpticalFlowSF(x,y,z,3,2,4);
            opencv_core.Mat img = ximg, oimg = yimg;
            opencv_core.Mat gimg = new opencv_core.Mat(), goimg = new opencv_core.Mat();
            int side = 5;
            double [][] Ix = new double[img.rows()][img.cols()], Iy = new double[img.rows()][img.cols()], It = new double[img.rows()][img.cols()];
            double [][] Vx = new double[img.rows()][img.cols()], Vy = new double[img.rows()][img.cols()];
            double [][] Vvx = new double[img.rows()][img.cols()], Vvy = new double[img.rows()][img.cols()];
            opencv_imgproc.cvtColor(img,gimg,opencv_imgproc.COLOR_BGR2GRAY);
            opencv_imgproc.cvtColor(oimg,goimg,opencv_imgproc.COLOR_BGR2GRAY);
            for(int i=0;i<img.rows();i++) {
                for (int j = 0; j < img.cols(); j++) {
                    if(i==0){
                        Iy[i][j] = (gimg.ptr(i+1,j).get()-gimg.ptr(i,j).get())*2;
                    }else if(i==img.rows()-1){
                        Iy[i][j] = (gimg.ptr(i,j).get()-gimg.ptr(i-1,j).get())*2;
                    }else{
                        Iy[i][j] = (gimg.ptr(i+1,j).get()-gimg.ptr(i-1,j).get());
                    }
                    if(j==0){
                        Ix[i][j] = (gimg.ptr(i,j+1).get()-gimg.ptr(i,j).get())*2;
                    }else if(j==img.cols()-1){
                        Ix[i][j] = (gimg.ptr(i,j).get()-gimg.ptr(i,j-1).get())*2;
                    }else{
                        Ix[i][j] = (gimg.ptr(i,j+1).get()-gimg.ptr(i,j-1).get());
                    }
                    It[i][j] = goimg.ptr(i,j).get()-gimg.ptr(i,j).get();
                }
            }
        /* LK Optical Flow */
            double [][] gaussian = new double[side*2+1][side*2+1];
            double sigma = 3,minx = 100000000, maxx = -10000000,miny = 1000000000,maxy = -1000000000;
            for(int i=0;i<side*2+1;i++)
                for(int j=0;j<side*2+1;j++){
                    double dis = (i-side)*(i-side)+(j-side)*(j-side);
                    gaussian[i][j] = Math.exp(-dis/(2*sigma*sigma))/(sigma*Math.sqrt(Math.PI*2));
                }
            for(int i=0;i<img.rows();i++){
                for(int j=0;j<img.cols();j++){
                    double a=0,b=0,c=0,d=0,e=0,f=0;
                    for(int x=0;x<side*2+1;x++)
                        for(int y=0;y<side*2+1;y++){
                            int tx = x+i-side, ty = y+j-side;
                            if(tx>=0&&tx<img.rows()&&ty>=0&&ty<img.cols()) {
                                a += gaussian[x][y]*Ix[tx][ty]*Ix[tx][ty];
                                b += gaussian[x][y]*Ix[tx][ty]*Iy[tx][ty];
                                c += gaussian[x][y]*Ix[tx][ty]*It[tx][ty];
                                d += gaussian[x][y]*Ix[tx][ty]*Iy[tx][ty];
                                e -= gaussian[x][y]*Iy[tx][ty]*Iy[tx][ty];
                                f -= gaussian[x][y]*Iy[tx][ty]*It[tx][ty];
                            }
                        }
                    Vy[i][j] = (f-c*d/a)/(e-b*d/a);
                    Vx[i][j] = (c-b*Vy[i][j])/a;
                    //System.out.printf("[  %.5f  %.5f  ]",Vx[i][j],Vy[i][j]);
                    //if(Math.abs(Vx[i][j])>5.0){Vx[i][j] = 0;Vy[i][j] = 0;}
                    //if(Math.abs(Vy[i][j])>5.0){Vx[i][j] = 0;Vy[i][j] = 0;}
                    if(Vy[i][j]>maxy)maxy=Vy[i][j];
                    if(Vx[i][j]>maxx)maxx=Vx[i][j];
                    if(Vy[i][j]<miny)miny=Vy[i][j];
                    if(Vx[i][j]<minx)minx=Vx[i][j];
                }
            }
            //System.out.printf(" !!!  %.5f %.5f !!!!\n",max,min);
            opencv_core.Mat line = ximg.clone();
            opencv_core.Scalar cl;
            for(int i=0;i<ximg.rows();i++)
                for(int j=0;j<ximg.cols();j++){
                    int s=0;
                    Vvx[i][j]=0;
                    Vvy[i][j]=0;
                    for(int x=i-2;x<=i+2;x++)
                        for(int y=j-2;y<=j+2;y++)
                            if(x>=0&&x<ximg.rows()&&y>=0&&y<ximg.cols()){
                                s++;
                                Vvx[i][j]+=Vx[x][y];
                                Vvy[i][j]+=Vy[x][y];
                            }
                    Vvx[i][j]/=s;
                    Vvy[i][j]/=s;
                }
            for(int i=0;i<ximg.rows();i++)
                for(int j=0;j<ximg.cols();j++){
                        //System.out.printf("! %d %d !\n",i,j);
                    /*if(Math.pow((double)zzz.get(0).ptr(i,j).get(),2)+Math.pow((double)zzz.get(1).ptr(i,j).get(),2)>100&&Math.pow((double)zzz.get(0).ptr(i,j).get(),2)+Math.pow((double)zzz.get(1).ptr(i,j).get(),2)<1000)*/{
                        double tt = Vy[i][j]*Vy[i][j]+Vx[i][j]*Vx[i][j];
                        double r = Math.atan2(Vy[i][j],Vx[i][j]),g=0,b=0;
                        cl = new opencv_core.Scalar(opencv_core.CV_RGB((int)(Math.sin(r)*255),(int)(Math.cos(r)*255),0));
                        if(tt>0.00001&&tt<100&&(Math.abs(Vy[i][j])>1e-9||Math.abs(Vx[i][j])>1e-9)&&i%7==1&&j%7==1){
                            opencv_core.circle(line, new opencv_core.Point(j,i),2,cl);
                            opencv_core.line(line, new opencv_core.Point(j, i), new opencv_core.Point((int) (j + Math.cos(r)*10), (int) (i + Math.sin(r)*10)), cl);

                        }
                    }
                }
            opencv_highgui.imwrite( "C:/testing/output/line.jpg",line);

            /*opencv_core.Mat mag = new opencv_core.Mat(),angle = new opencv_core.Mat();
            opencv_core.cartToPolar(zzz.get(0), zzz.get(1), mag, angle, true);*/
            /*opencv_core.Mat [] _hsv = new opencv_core.Mat[3];

            opencv_core.MatVector ot = new opencv_core.MatVector(3);
            opencv_core.Mat hsv = new opencv_core.Mat();*/

            /*
            DoublePointer mag_max = new DoublePointer(),mag_min = new DoublePointer();

            for(int i=0;i<mag.rows();i++){
                for(int j=0;j<mag.cols();j++){
                    System.out.printf("!%.2f! ",(float)mag.ptr(i,j).get());
                }
                System.out.println();
            }
            opencv_core.minMaxLoc(mag, mag_min, mag_max, null, null, mag);
            double xmax = mag_max.get(),xmin = mag_min.get();
            mag.convertTo(mag,opencv_core.CV_32F,1.0/(xmax-xmin),-xmin/(xmax-xmin));*/
            //opencv_core.normalize(mag, mag, 0, 1, opencv_core.NORM_MINMAX, -1, mag);
            /*_hsv[0] = zzz.get(0).clone();
            _hsv[1] = new opencv_core.Mat(_hsv[0].rows(),_hsv[0].cols(),opencv_core.CV_32FC1, (new opencv_core.Scalar(1)));
            //opencv_core.absdiff(zzz.get(1), new opencv_core.Mat(_hsv[0].rows(),_hsv[0].cols(),opencv_core.CV_32F,new opencv_core.Scalar(0.0)),_hsv[2]);
            for(int i=0;i<_hsv[1].rows();i++){
                for(int j=0;j<_hsv[1].cols();j++){
                    System.out.printf("!%.2f! ",(float)_hsv[1].ptr(i,j).get());
                }
                System.out.println();
            }

            ot.put(0, _hsv[0]);

            ot.put(2, _hsv[2]);

            ot.put(1, _hsv[1]);

            opencv_core.merge(ot, hsv);
            opencv_imgproc.cvtColor(hsv, z1, opencv_imgproc.COLOR_HSV2BGR);*/
            /*z1.convertTo(z1,opencv_core.CV_8U);
            for(int i=0;i<z1.rows();i++){
                for(int j=0;j<z1.cols();j++){
                    System.out.printf("!%.2f! ",(float)z1.ptr(i,j).get());
                }
                System.out.println();
            }
            System.out.println(z1);*/
            collector.emit(new Values(new Integer(tuple.getIntegerByField("ID")), new tool.Serializable.Mat(line), new tool.Serializable.Mat(ximg), new tool.Serializable.Mat(yimg)));
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("ID", "Flow", "First", "Last"));
        }
    }
    public static class MergeBolt extends BaseBasicBolt{
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector){
            opencv_core.IplImage fkImpage = new opencv_core.IplImage();
            opencv_core.Mat x = ((tool.Serializable.Mat)tuple.getValueByField("First")).toJavaCVMat(),y = ((tool.Serializable.Mat)tuple.getValueByField("Last")).toJavaCVMat(),z = ((tool.Serializable.Mat)tuple.getValueByField("Flow")).toJavaCVMat(),z1 = new opencv_core.Mat();

        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){
            outputFieldsDeclarer.declare(new Fields("ID","First"));
        }
    }
    public static class FrameToVideoBolt extends BaseRichBolt{
        int LastID;
        int Thres;
        Map<Integer,opencv_core.Mat> matMap;
        OutputCollector _collector;
        opencv_highgui.VideoWriter out;
        public FrameToVideoBolt(){}
        @Override
        public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector){
            opencv_core.IplImage fkImpage = new opencv_core.IplImage();
            _collector = outputCollector;
            LastID = 0;
            matMap = new TreeMap<Integer,opencv_core.Mat>();
            out = new opencv_highgui.VideoWriter("C:/testing/output/1.avi",opencv_highgui.CV_FOURCC_DEFAULT,20.0,new opencv_core.Size(426,240));
            Thres = 20;
        }
        public void execute(Tuple tuple){
            int ID = tuple.getIntegerByField("ID").intValue();
            if(out.isOpened()){System.out.println("NOW is "+String.valueOf(ID));
            while(!matMap.isEmpty()&&matMap.entrySet().iterator().next().getKey().intValue() < ID - Thres){
                out.write(matMap.entrySet().iterator().next().getValue());
                LastID = (matMap.entrySet().iterator().next()).getKey().intValue();
                opencv_highgui.imwrite("C:/testing/output/" + String.valueOf(LastID) + ".jpg", matMap.entrySet().iterator().next().getValue());
                System.out.printf("Now is %d\n",LastID);
                matMap.remove(new Integer(LastID));
            }
            if(ID>LastID&&!matMap.containsKey(new Integer(ID))){
                opencv_core.Mat x1 = ((tool.Serializable.Mat)tuple.getValueByField("Flow")).toJavaCVMat();

                matMap.put(ID, ((tool.Serializable.Mat) tuple.getValueByField("Flow")).toJavaCVMat());
            }
            while(!matMap.isEmpty()&&matMap.entrySet().iterator().next().getKey().intValue() == LastID + 1) {
                out.write(matMap.entrySet().iterator().next().getValue());
                LastID = matMap.entrySet().iterator().next().getKey().intValue();
                System.out.printf("Now is %d\n", LastID);
                opencv_highgui.imwrite("C:/testing/output/" + String.valueOf(LastID) + ".jpg", matMap.entrySet().iterator().next().getValue());
                matMap.remove(new Integer(LastID));
            }}
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){
            outputFieldsDeclarer.declare(new Fields());
        }
        @Override
        public void cleanup(){
            while(!matMap.isEmpty()){
                out.write(matMap.entrySet().iterator().next().getValue());
                LastID = (matMap.entrySet().iterator().next()).getKey().intValue();
                opencv_highgui.imwrite("C:/testing/output/" + String.valueOf(LastID) + ".jpg", matMap.entrySet().iterator().next().getValue());
                System.out.printf("Now is %d\n",LastID);
                matMap.remove(new Integer(LastID));
            }
            out.release();
        }
    }
    /*public static class TestImageOutputBolt extends BaseBasicBolt{
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector){
            opencv_core.IplImage fkImpage = new opencv_core.IplImage();
            tool.Serializable.Mat x = (tool.Serializable.Mat)(tuple.getValueByField("Output"));
            //opencv_core.Mat ker = new opencv_core.Mat(opencv_core.Mat.zeros(x.toJavaCVMat().size(),x.toJavaCVMat().type()));
            opencv_core.Mat y = x.toJavaCVMat();
            opencv_imgproc.cvtColor(x.toJavaCVMat(),y,opencv_imgproc.CV_RGB2HSV);
            opencv_highgui.imwrite("C:/testing/output/"+tuple.getString(1), y);
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){}
    }*/
    public static void main(String args[])throws Exception{
        TopologyBuilder builder = new TopologyBuilder();
        /*builder.setSpout("spout1", new TestImageReadSpout(), 1);
        builder.setBolt("bolt2", new FilterBolt(),2).shuffleGrouping("spout1");
        builder.setBolt("bolt1",new TestImageOutputBolt(),2).shuffleGrouping("bolt2");*/
        /*builder.setSpout("spout1", new VideoDoubleFrameSpout(),1);
        builder.setBolt("filterbolt", new FilterBolt(),3).shuffleGrouping("spout1");
        builder.setBolt("bolt1",new OpticalFlowBolt(),3).shuffleGrouping("filterbolt");
        builder.setBolt("bolt2",new FrameToVideoBolt(),1).shuffleGrouping("bolt1");*/
        builder.setSpout("spout1", new Video15FrameSpout(), 1);
        //builder.setBolt("bolt1",new DevideImageBolt.DenseTrajectBolt(),1).shuffleGrouping("spout1");
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_DEBUG, true);
        conf.setStatsSampleRate(1.0);
        conf.setMaxSpoutPending(2);
        if (args!=null && args.length > 0){
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0],conf,builder.createTopology());
        }else{
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("imageread", conf, builder.createTopology());
            Thread.sleep(30000);
            cluster.shutdown();
        }
    }
}
