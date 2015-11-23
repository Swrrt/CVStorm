package topology;
import TranslateFramework.HOGBolt;
import TranslateFramework.NNBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.*;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.*;
import tool.Serializable;

import java.io.*;
import java.util.*;
import libsvm.*;
/**
 * Created by swrrt on 27/8/2015.
 */
public class SIFTSVMDEMO {
    public static class ImageReadSpout extends BaseRichSpout{
        SpoutOutputCollector collector;
        Set <String> Files;
        int ID;
        boolean codebookover, trainover;
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector _collector){
            collector = _collector;
            Files = new HashSet<String>();
            ID = 0;
            File xxx = new File("C:/testing/SIFTDEMO/codebook.dat");
            codebookover = xxx.exists();
            xxx = new File("C:/testing/SIFTDEMO/trainedSVM.dat");
            trainover = xxx.exists();
        }
        @Override
        public void nextTuple(){
            opencv_core.IplImage fkImpage = new opencv_core.IplImage();
            String pathname = new String("C:/testing/SIFTDEMO/");
            File [] tfiles = (new File(pathname)).listFiles();
            int label;
            if(!codebookover){
                tfiles = (new File(pathname+"codebook/")).listFiles();
                codebookover = true;
                for(File t:tfiles) {
                    if (t.isFile() && !Files.contains("codebook/"+t.getName())) {
                        Files.add("codebook/"+t.getName());
                        ID++;
                        System.out.println("!!!!!!!!!" + ID + pathname + t.getName() + "!!!!!!!!!!!!");
                        opencv_core.Mat image = opencv_highgui.imread(pathname +"codebook/"+ t.getName(),1).clone();
                        label = Integer.parseInt(t.getName().substring(t.getName().length()-5,t.getName().length()-4));
                        collector.emit(new Values(ID, new Serializable.Mat(image), true, false,label),"1");
                        codebookover = false;
                        break;
                    }
                }
            }else if(!trainover){
                tfiles = (new File(pathname+"train/")).listFiles();
                trainover = true;
                for(File t:tfiles) {
                    if (t.isFile() && !Files.contains("train/"+t.getName())) {
                        Files.add("train/"+t.getName());
                        ID++;
                        System.out.println("!!!!!!!!!" + ID + pathname + t.getName() + "!!!!!!!!!!!!");
                        opencv_core.Mat image = opencv_highgui.imread(pathname +"train/"+ t.getName(),1).clone();
                        //System.out.println("[ wth ]"+t.getName()+"|||"+t.getName().substring(t.getName().length()-5,t.getName().length()-4));
                        label = Integer.parseInt(t.getName().substring(t.getName().length()-5,t.getName().length()-4));
                        collector.emit(new Values(ID, new Serializable.Mat(image), false, true,label),"1");
                        trainover = false;
                        break;
                    }
                }
            }else{
                tfiles = (new File(pathname+"testing/")).listFiles();
                for(File t:tfiles) {
                if (t.isFile() && !Files.contains("testing/"+t.getName())) {
                    Files.add("testing/"+t.getName());
                    ID++;
                    System.out.println("!!!!!!!!!" + ID + pathname + t.getName() + "!!!!!!!!!!!!");
                    opencv_core.Mat image = opencv_highgui.imread(pathname +"testing/" + t.getName(),1).clone();
                    label = 0;
                    collector.emit(new Values(ID, new Serializable.Mat(image), false, false,label),"1");
                    break;
                }
            }}
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){
            outputFieldsDeclarer.declare(new Fields("ID", "Image", "IsCodebook", "IsTraining","Label"));
        }
    }
public static class SiftBolt extends BaseBasicBolt{
        BasicOutputCollector collector;
        List<Integer> KeyPointx;
        List<Integer> KeyPointy;
        boolean iscodebook,istraining;
        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector){
            collector = basicOutputCollector;
            opencv_core.IplImage fkImpage = new opencv_core.IplImage();
            opencv_core.Mat x = ((tool.Serializable.Mat)tuple.getValueByField("Image")).toJavaCVMat();
            iscodebook = tuple.getBooleanByField("IsCodebook");
            istraining = tuple.getBooleanByField("IsTraining");
            KeyPointx = new ArrayList<Integer>();
            KeyPointy = new ArrayList<Integer>();
            MySift(tuple, x, (int) tuple.getValueByField("ID"));
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){
            outputFieldsDeclarer.declare(new Fields("ID", "Image", "IsCodebook", "IsTraining", "Feature", "Label"));
        }
        public void MySift(Tuple tuple,opencv_core.Mat img,int ID){
            opencv_core.Mat oimg = img.clone(), toimg = img.clone();
            opencv_imgproc.cvtColor(img, img, opencv_imgproc.COLOR_BGR2GRAY);
            opencv_imgproc.GaussianBlur(img,img,new opencv_core.Size(5,5),0.2);
            //opencv_highgui.imwrite("C:/testing/SIFTDEMO/output/1.jpg",img);

            int height = img.arrayHeight(),width = img.arrayWidth(),channels = img.channels();
            int octaves = 4,scales = 5;
            /* generate scale space */
            opencv_core.IplImage [][] scale_space = new opencv_core.IplImage[octaves][scales];
            scale_space[0][0] = img.asCvMat().asIplImage().clone();
            for(int i = 1; i < octaves; i++){
                opencv_core.IplImage prev = scale_space[i-1][0].clone();
                int new_h = (int)Math.ceil(prev.height()/Math.sqrt(2)),new_w = (int)Math.ceil(prev.width()/Math.sqrt(2));
                opencv_core.IplImage resized = opencv_core.cvCreateImage(new opencv_core.CvSize(opencv_core.cvSize(new_w,new_h)),prev.depth(),prev.nChannels()).clone();
                opencv_imgproc.cvResize(prev,resized);
                scale_space[i][0] = resized.clone();
            }
            double sigma_step = 1 / Math.sqrt(2.0);
            double k_step = 1 / sigma_step;
            for(int i = 0;i<octaves;i++){
                for(int j=1;j<scales;j++){
                    scale_space[i][j] = GaussianBlur(scale_space[i][j - 1], sigma_step);
                }
                sigma_step *= k_step;
            }
            opencv_core.IplImage [][] diff_of_gauss = difference_scale_space(scale_space, octaves, scales);
            opencv_core.IplImage [][] keypoints =  keypoints_dog(diff_of_gauss, octaves,scales);
            //opencv_highgui.cvNamedWindow("mainWin", opencv_highgui.CV_WINDOW_AUTOSIZE);
            //opencv_highgui.cvMoveWindow("mainWin",400,30);
            //opencv_highgui.cvShowImage("mainWin", scale_space[3][0]);

            BytePointer xxx = keypoints[3][0].imageData();
            for(int i=0;i<keypoints[3][0].height();i++){
                for(int j=0;j<keypoints[3][0].width();j++) {
                   if ((int) xxx.get(i * keypoints[3][0].width() + j) > 0) {
                       int x = (int) (((double) i) / keypoints[3][0].height() * oimg.rows()), y = (int) (((double) j) / keypoints[3][0].width() * oimg.cols());
                       int sss = 0, ss = 0;
                       for (int xx = x - 2; xx <= x + 2; xx++)
                           for (int yy = y - 2; yy <= y + 2; yy++)
                               if (xx >= 0 && xx <= oimg.rows() && yy >= 0 && yy <= oimg.cols()) {
                                   ss++;
                                   sss+=((int)(img.ptr(x,y).get())-(int)(img.ptr(xx,yy).get()))*((int)(img.ptr(x,y).get())-(int)(img.ptr(xx,yy).get()));
                               }
                       if(((double)sss)/ss > 5.0){
                           opencv_core.circle(oimg, new opencv_core.Point(y, x), 3, new opencv_core.Scalar(128, 128, 128, 0));
                           KeyPointx.add(y);
                           KeyPointy.add(x);
                       }
                   }
                }
            }
            List<List<Double>> feature = new ArrayList<>();
            for(int i=0;i<KeyPointx.size();i++){
                List<Double> A = new ArrayList<>();
                if(KeyPointx.get(i)>=2&&KeyPointy.get(i)>=2&&KeyPointx.get(i)+2<toimg.cols()&&KeyPointy.get(i)+2<toimg.rows()){
                    for(int dx=KeyPointx.get(i)-2;dx<=KeyPointx.get(i)+2;dx++)
                        for(int dy=KeyPointy.get(i)-2;dy<=KeyPointy.get(i)+2;dy++)
                            for(int z=0;z<3;z++)
                                A.add((double) img.ptr(dy).get(dx * 3 + z));
                    feature.add(A);
                }
            }
            collector.emit(new Values(ID,new tool.Serializable.Mat(toimg),iscodebook,istraining, feature , tuple.getIntegerByField("Label")));
            opencv_highgui.imwrite("C:/testing/SIFTDEMO/output/" + String.valueOf(ID) + ".jpg", oimg);

        }
        public opencv_core.IplImage [][] keypoints_dog(opencv_core.IplImage [][] dog, int octaves, int scales){
            --scales;
            opencv_core.IplImage [][] keypoints = new opencv_core.IplImage[octaves][scales-2];
            for(int i=0;i<octaves;i++){
                for(int j=0;j<scales-2;j++){
                    opencv_core.CvSize sz = opencv_core.cvSize(dog[i][0].width(),dog[i][0].height());
                    keypoints[i][j] = opencv_core.cvCreateImage(sz,dog[i][0].depth(),1);
                }
            }
            for(int i=0;i<octaves;i++){
                for(int j=1;j<scales-1;j++){
                    opencv_core.IplImage curimg = dog[i][j];
                    BytePointer data = curimg.imageData();
                    int height = curimg.height(), width = curimg.width(), width_sz = curimg.widthStep();
                    BytePointer keypoints_data = keypoints[i][j-1].imageData();

                    for(int a=1;a<height-1;a++){
                        for(int b=1;b<width_sz-1;b++){
                            byte curr_px = data.get(a*width_sz+b);
                            boolean max = true, min = true;
                            for(int d=-1;d<=1;d++){
                                for(int dd=-1;dd<=1;dd++){
                                    for(int ddd=-1;ddd<=1;ddd++)
                                        if(d!=0||dd!=0||ddd!=0){
                                            opencv_core.IplImage cur_test_img = dog[i][j+d];
                                            BytePointer cur_test = cur_test_img.imageData();
                                            if(curr_px<cur_test.get((a+dd)*width_sz+(b+ddd)))max = false;
                                            if(curr_px>cur_test.get((a+dd)*width_sz+(b+ddd)))min = false;
                                        }
                                }
                            }
                            if(max||min){
                                keypoints_data.put((a*width_sz)+b, data.get(a*width_sz+b));
                            }
                        }
                    }
                }
            }
            return keypoints;
        }
        public opencv_core.IplImage difference(opencv_core.IplImage img1, opencv_core.IplImage img2){
            int height = img1.height(),width = img1.width();
            BytePointer data1 = img1.imageData(),data2 = img2.imageData();
            opencv_core.CvSize sz = opencv_core.cvSize(width, height);
            opencv_core.IplImage dif = opencv_core.cvCreateImage(sz,img1.depth(),1);
            BytePointer diff = dif.imageData();
            int width_step = img1.widthStep();
            for(int i=0;i<height;i++)
                for(int j=0;j<width;j++){
                    int curr_px = i*width+j;
                    diff.put(curr_px,(byte)Math.abs(data1.get(curr_px)-data2.get(curr_px)));
                    if(diff.get(curr_px)<16)
                        diff.put(curr_px,(byte)(diff.get(curr_px)*diff.get(curr_px)));
                    else
                        diff.put(curr_px, (byte)255);
                    if(diff.get(curr_px)<10)
                        diff.put(curr_px, (byte)0);
                }
            return dif;
        }
        public opencv_core.IplImage [] difference_octave(opencv_core.IplImage [] octave, int scales){
            opencv_core.IplImage [] dog_octave = new opencv_core.IplImage[scales-1];
            for(int i=0;i<scales-1;i++){
                dog_octave[i]  = difference(octave[i], octave[i + 1]);
            }
            return dog_octave;
        }
        public opencv_core.IplImage [][] difference_scale_space(opencv_core.IplImage [][] scale_space, int octaves, int scales){
            opencv_core.IplImage [][] dog = new opencv_core.IplImage[octaves][scales];
            for(int i=0;i<octaves;i++){
                dog[i] = difference_octave(scale_space[i],scales);
            }
            return dog;
        }
        public opencv_core.IplImage GaussianBlur(opencv_core.IplImage img, double sigma) {
            opencv_core.IplImage timg = img.clone();
            BytePointer data = timg.imageData();
            BytePointer odata = img.imageData();
            int step = timg.widthStep();
            int kernalsize = (int) Math.ceil(6 * sigma);
            int center = kernalsize / 2;
            double normalfactor = 2.0 * Math.PI * sigma * sigma;
            double[][] kernal = new double[kernalsize][kernalsize];
            for (int i = 0; i < kernalsize; i++)
                for (int j = 0; j < kernalsize; j++) {
                    double x = Math.abs(i - center);
                    double y = Math.abs(j - center);
                    kernal[i][j] = Math.exp(-1.0 * (x * x + y * y) / (2 * sigma * sigma));
                    kernal[i][j] /= normalfactor;
                }
            int channels = timg.nChannels();
            for(int i=0;i<timg.height();i++){
                for(int j=0;j<timg.width();j++){
                    for(int k=0;k<channels;k++){
                        double kernal_total = 0;
                        for(int a=0;a<kernalsize;a++){
                            for(int b=0;b<kernalsize;b++){
                                int delta_j = b-center;
                                int delta_i = a-center;
                                if(i+delta_i>=0&&i+delta_i<timg.height()&&j+delta_j>=0&&j+delta_j<timg.width()){
                                    kernal_total += kernal[a][b];
                                }
                            }
                        }
                        double compensation = 1.0/kernal_total;
                        double new_value = 0;
                        for(int a=0;a<kernalsize;a++){
                            for(int b=0;b<kernalsize;b++){
                                int delta_j = b-center;
                                int delta_i = a-center;
                                if(i+delta_i>=0&&i+delta_i<timg.height()&&j+delta_j>=0&&j+delta_j<timg.width()){
                                    double curr = odata.get((i+delta_i)*step+(j+delta_j)*channels+k);
                                    new_value += curr;
                                }
                            }
                        }
                        data.put(i*step+j*channels+k, (byte)new_value);
                    }
                }
            }
            return timg;
        }
    }

    public static class BOW extends BaseRichBolt{
            boolean training;
            int dim = 18;
            int N = 200;
            List<List<Double>> A;
            List<List<Double>> trainData;
            OutputCollector collector;
            @Override
            public void prepare(Map conf, TopologyContext context, OutputCollector _collector){
                collector = _collector;
                opencv_core.IplImage fkImpage = new opencv_core.IplImage();

                A = new ArrayList<List<Double>>();
                File xxx = new File("C:/testing/SIFTDEMO/codebook.dat");
                System.out.println("[     ]     Something happend?");
                if(xxx.exists()){
                    training = false;
                    try{
                        Scanner in = new Scanner(xxx);
                        double t;
                        for(int i=0;i<N;i++){
                            List<Double> x = new ArrayList<Double>();
                            for(int j=0;j<dim;j++){
                                t = in.nextDouble();
                                x.add(t);
                            }
                            A.add(x);
                        }
                    }catch (Exception x){};
                }else{
                    training = true;
                    Random tt = new Random();
                    trainData = new ArrayList<List<Double>>();
                    for(int i=0;i<N;i++){
                        List<Double> x = new ArrayList<>();
                        for(int j=0;j<dim;j++){
                            x.add(new Double((double)tt.nextInt(10000)/10000.0));
                        }
                        A.add(x);
                    }
                }
            }
            @Override
            public void execute(Tuple tuple){
                opencv_core.Mat img = ((tool.Serializable.Mat) tuple.getValueByField("Image")).toJavaCVMat();
                System.out.println("SHIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIT\n");

                //List<Integer> keypointx = (ArrayList<Integer>)tuple.getValueByField("KeyPointx"),keypointy = (ArrayList<Integer>)tuple.getValueByField("KeyPointy");
                List<List<Double>> patch = (List<List<Double>>)tuple.getValueByField("Feature");
                /*int n = keypointx.size();
                System.out.printf("[ Debug ] kx %d ky %d\n",n,keypointy.size());
                for(int i=0;i<n;i++){
                    List<Double> pat = new ArrayList<Double>();
                    if(keypointx.get(i)-2>=0 && keypointx.get(i)+2<img.cols() && keypointy.get(i)-2>=0 && keypointy.get(i)+2<img.rows()) {
                        for (int x = keypointx.get(i) - 2; x <= keypointx.get(i) + 2; x++)
                            for (int y = keypointy.get(i) - 2; y <= keypointy.get(i) + 2; y++)
                                for(int z = 0;z<3;z++){
                                    //System.out.printf("[ Debug ] x %d y %d z %d !!!\n",x,y,z);
                                    pat.add((double) img.ptr(y).get(x * 3 + z));
                                }
                        patch.add(pat);
                    }
                }*/
                //System.out.println("[WTF???]  "+ training +tuple.getBooleanByField("IsCodebook"));
                if(training&&tuple.getBooleanByField("IsCodebook").booleanValue()){
                    for(int i=0;i<patch.size();i++){
                        trainData.add(patch.get(i));
                    }
                }else if(training&&!tuple.getBooleanByField("IsCodebook").booleanValue()){                              /*   K-means here!  */
                    int iter = 50;
                    List<Integer> ss = new ArrayList<Integer>();
                    List<List<Double>> sx = new ArrayList<List<Double>>();
                    for(int i=0;i<N;i++){
                        List<Double> t = new ArrayList<Double>();
                        for(int j=0;j<dim;j++)t.add(0.0);
                        sx.add(t);
                        ss.add(0);
                    }

                    int tn = trainData.size();
                    while(iter>0){
                        iter--;
                        System.out.println("[ BOW iteration ] " + iter);
                        for(int i = 0;i < tn;i++){
                            double min = 1e10;
                            int mini=-1;
                            for(int j=0;j < N;j++){
                                double s = 0;
                                for(int k=0;k<dim;k++){
                                    double t = trainData.get(i).get(k) - A.get(j).get(k);
                                    s+=t*t;
                                }
                                //System.out.println(s);
                                if(s<min){
                                    min = s;
                                    mini = j;
                                }
                            }

                            ss.set(mini,ss.get(mini)+1);
                            for(int j=0;j<dim;j++)
                                sx.get(mini).set(j,sx.get(mini).get(j)+trainData.get(i).get(j));
                        }
                        //System.out.printf("%d %d %d\n",A.size(),sx.size(),ss.size());
                        for(int i=0;i<N;i++){
                            for(int j=0;j<dim;j++) {
                                if(ss.get(i)>0)A.get(i).set(j, sx.get(i).get(j) / ss.get(i));
                                sx.get(i).set(j, 0.0);
                            }
                            ss.set(i,0);
                        }
                    }
                    training = false;
                    try{
                        FileWriter x = new FileWriter("C:/testing/SIFTDEMO/codebook.dat",false);
                        for(int i=0;i<N;i++){
                            for(int j=0;j<dim;j++){
                                x.write(A.get(i).get(j).toString()+" ");
                            }
                            x.write("\n");
                        }
                        x.close();
                    }catch (Exception e){};

                }
                //System.out.println("[WTF???]  "+ training +tuple.getBooleanByField("IsCodebook"));
                if(!training&&!tuple.getBooleanByField("IsCodebook").booleanValue()){                                        /* Finding closest*/

                    List<Double> V = new ArrayList<Double>();
                    for(int i=0;i<N;i++)V.add(0.0);
                    int ss = 0;
                    for(int i = 0;i < patch.size();i++){
                        double min = 1e10;
                        int mini=-1;
                        for(int j=0;j < N;j++){
                            double s = 0;
                            for(int k=0;k<dim;k++){
                                double t = patch.get(i).get(k) - A.get(j).get(k);
                                s+=t*t;
                            }
                            if(s<min){
                                min = s;
                                mini = j;
                            }
                        }
                        if(mini!=-1){
                            V.set(mini,V.get(mini)+1);
                            ss++;
                        }
                    }
                    if(ss!=0)for(int i=0;i<N;i++)V.set(i,V.get(i)/ss);
                    try{
                        FileWriter f = new FileWriter("C:/testing/SIFTDEMO/output/"+tuple.getIntegerByField("ID").intValue()+".txt",false);
                        for(int i=0;i<N;i++){
                            f.write(V.get(i).toString()+" ");
                        }
                        f.close();
                    }catch (Exception e){}
                    collector.emit(new Values(tuple.getIntegerByField("ID"), tuple.getValueByField("Image"),tuple.getBooleanByField("IsTraining"),V,tuple.getIntegerByField("Label")));
                }
            }
            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer){
                declarer.declare(new Fields("ID","Image","IsTraining","Feature","Label"));
            }
            @Override
            public void cleanup(){

            }
    }
    public static class sparseae extends BaseRichBolt{
        List<double [][]> W;
        List<double[]> B;
        double lrate;
        boolean training;
        int N,M;
        int iter;
        List<List<Double>> trainData;
        List<double[][]> delta;
        List<Integer> Label;
        List<Serializable.Mat> img;
        OutputCollector collector;
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector _collector){
            collector = _collector;
            opencv_core.IplImage fkImpage = new opencv_core.IplImage();
            Label = new ArrayList<>();
            trainData = new ArrayList<>();
            N = 200;
            M = 50;
            lrate = 0.3;
            W = new ArrayList<>();
            delta = new ArrayList<>();
            B = new ArrayList<>();
            W.add(new double[M][N]);
            delta.add(new double[M][N]);
            B.add(new double[M]);
            W.add(new double[N][M]);
            delta.add(new double[N][M]);
            B.add(new double[N]);
            training = true;
            iter = 1000;
        }
        public double sigmod(double x){
            return 1.0/(1.0 + Math.exp(-x));
        }
        @Override
        public void execute(Tuple tuple){
            List<Double> x = (List<Double>)tuple.getValueByField("Feature");
            boolean is_training = tuple.getBooleanByField("IsTraining");
            if(training) {
                if (is_training) {
                    trainData.add(x);
                    Label.add(tuple.getIntegerByField("Label"));
                    img.add((tool.Serializable.Mat) tuple.getValueByField("Image"));
                }else{
                    for(int i=0;i<N;i++){
                        Random rand = new Random();
                        for(int j=0;j<M;j++){
                            W.get(0)[j][i]= rand.nextInt(10000)/5000.0 - 1.0;
                            delta.get(0)[j][i] = 0;
                            W.get(1)[i][j]= rand.nextInt(10000)/5000.0 - 1.0;
                            delta.get(1)[i][j] = 0;
                        }
                    }
                    double [] input = new double[N],s_output = new double[N], h_output = new double[M],output = new double[N];
                    double [] o_delta = new double[N], h_delta = new double[M];
                    while(iter>0){
                        iter--;
                        double error = 0;
                        for(int tt = 0;tt<trainData.size();tt++){
                            double max = 0, min = 0;
                            for(int i=0;i<N;i++){
                                input[i] = trainData.get(tt).get(i);
                                if(input[i]>max) max = input[i];
                                if(input[i]<min) min = input[i];
                            }
                            for(int i=0;i<N;i++){
                                input[i] = (input[i]-min)/(max-min);
                            }
                            for(int i=0;i<N;i++){
                                s_output[i] = trainData.get(tt).get(i);
                            }
                        for(int i=0; i<M;i++){
                            double sum = 0;
                            for(int j=0;j<N;j++){
                                sum+= W.get(0)[i][j]*input[j];
                            }
                            h_output[i] = sigmod(sum);
                        }
                        for(int i=0;i<N;i++){
                            double sum = 0;
                            for(int j=0;j<M;j++){
                                sum+= W.get(1)[i][j]*h_output[j];
                            }
                            output[i] = sigmod(sum);
                        }
                        for(int i=0;i<N;i++){
                            double t_error = s_output[i]-output[i];
                            o_delta[i] = -t_error*sigmod(output[i])*(1.0-sigmod(output[i]));
                            error+=t_error*t_error;
                        }
                        for(int i=0;i<M;i++){
                            double t_error = 0;
                            for(int j=0;j<N;j++){
                                t_error+=o_delta[j]*W.get(1)[j][i];
                            }
                            h_delta[i] = t_error*(1.0-h_output[i])*(1.0+h_output[i]);
                        }
                        for(int i=0;i<N;i++)
                            for(int j=0;j<M;j++){
                                delta.get(1)[i][j] = lrate*delta.get(1)[i][j] + lrate * o_delta[i] * h_output[j];
                                W.get(1)[i][j]-= delta.get(1)[i][j];
                            }
                        for(int i=0;i<M;i++)
                            for(int j=0;j<N;j++){
                                delta.get(0)[i][j] = lrate*delta.get(0)[i][j] + lrate * h_delta[i] * input [j];
                                W.get(0)[i][j] -= delta.get(0)[i][j];
                            }
                        }
                        System.out.printf("[ Autoencoder ] error: %.6f\n",error);
                    }
                    training = false;
                }
            }
            if(!training){

            }
        }
        @Override
        public void declareOutputFields (OutputFieldsDeclarer declarer){
            declarer.declare(new Fields("ID","Image","Feature","IsTraining","Label"));
        }
    }
    public static class SVM extends BaseRichBolt{
        OutputCollector collector;
        boolean training;
        svm_model model;
        List<double[]> trainset;
        List<Integer> label;
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector _collector){
            collector = _collector;
            opencv_core.IplImage fkImpage = new opencv_core.IplImage();
            String filename = new String("C:/testing/SIFTDEMO/trainedSVM.dat");
            if((new File(filename)).exists()){
                try{
                    model = svm.svm_load_model(filename);
                }catch (Exception e){};
                training = false;
            }else{
                training = true;
                trainset = new ArrayList<double[]>();
                label = new ArrayList<>();
            }
        }
        @Override
        public void execute(Tuple tuple){
            opencv_core.Mat img = ((tool.Serializable.Mat)tuple.getValueByField("Image")).toJavaCVMat();
            List<Double> V = (ArrayList<Double>)tuple.getValueByField("Feature");
            if(training&&tuple.getBooleanByField("IsTraining")){
                double[] y = new double[V.size()];
                for(int i=0;i<V.size();i++)y[i] = V.get(i);
                trainset.add(y);
                label.add(tuple.getIntegerByField("Label"));
            }else if(training){
                double[] y = new double[V.size()];
                for(int i=0;i<V.size();i++)y[i] = V.get(i);
                trainset.add(y);
                label.add(tuple.getIntegerByField("Label"));
                svm_problem problem = new svm_problem();
                problem.l = trainset.size();
                problem.y = new double[problem.l];
                problem.x = new svm_node[problem.l][];
                for(int i=0;i<trainset.size();i++){
                    problem.x[i] = new svm_node[V.size()];
                    for(int j=0;j<V.size();j++) {
                        svm_node node = new svm_node();
                        node.index = j;
                        node.value = trainset.get(i)[j];
                        problem.x[i][j] = node;
                    }
                    problem.y[i] = (double)label.get(i);
                }
                svm_parameter parameter = new svm_parameter();
                parameter.probability = 1;
                parameter.gamma = 0.5;
                parameter.nu = 0.5;
                parameter.C = 1;
                parameter.svm_type = svm_parameter.C_SVC;
                parameter.kernel_type = svm_parameter.RBF;
                parameter.cache_size = 1024;
                parameter.eps = 0.0001;
                model = svm.svm_train(problem,parameter);
                training = false;
                try{
                    svm.svm_save_model("C:/testing/SIFTDEMO/trainedSVM.dat",model);
                }catch(Exception e){};
            }
            if(!training && !tuple.getBooleanByField("IsTraining")){
                svm_node[] nodes = new svm_node[V.size()];
                for(int i=0;i<V.size();i++){
                    svm_node node = new svm_node();
                    node.index = i;
                    node.value = V.get(i);
                    nodes[i] = node;
                }
                int totalClasses = 2;
                int[] labels = new int[totalClasses];
                svm.svm_get_labels(model,labels);
                double[] prob_e = new double[totalClasses];
                double v = svm.svm_predict_probability(model,nodes,prob_e);
                try{
                    FileWriter fileWriter = new FileWriter("C:/testing/SIFTDEMO/output/"+tuple.getIntegerByField("ID").toString()+"class.txt",false);
                    for(int i=0;i<totalClasses;i++)
                        fileWriter.write("Class "+String.valueOf(i)+" : "+String.valueOf(prob_e[i])+"\n");
                    fileWriter.write("Class is "+String.valueOf(v)+"\n");
                    fileWriter.close();
                }catch (Exception e){};
            }
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer){

        }
    }
    public static void main(String args[])throws Exception{
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("1", new ImageReadSpout(), 1);
        //builder.setBolt("2",new HOGBolt(),1).shuffleGrouping("1");
        builder.setBolt("3",new BOW(),1).shuffleGrouping("2");
        builder.setBolt("4",new NNBolt(),1).shuffleGrouping("3");
        //builder.setBolt("4",new SVM(),1).shuffleGrouping("3");
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_DEBUG, false);
        //conf.setMaxSpoutPending(10);
        conf.setStatsSampleRate(1.0);
        conf.setMessageTimeoutSecs(12000);
        //conf.put(Config.NIMBUS_TASK_TIMEOUT_SECS,"60");
        if (args!=null && args.length > 0){
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0],conf,builder.createTopology());
        }else{
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("imageread", conf, builder.createTopology());
            //Thread.sleep(10000);
            //cluster.shutdown();
        }
    }
}

