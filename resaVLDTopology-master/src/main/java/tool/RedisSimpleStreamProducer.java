package tool;

import generateTraj.helperFunctions;
import org.bytedeco.javacpp.opencv_core;
import redis.clients.jedis.Jedis;
import topology.StreamFrame;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Created by Tom Fu
 * RedisStreamProducerBeta keeps a timer for each frame, if the expected frame is late, it starts the time and wait until timeout,
 * then it simply drops this frame and come to the next expected frame. (suitable for loss insensitive application)
 */
public class RedisSimpleStreamProducer implements Runnable {
    /**
     * Ordered queue for putting frames in order
     */
    private PriorityQueue<GeneralizedStreamFrame> stream;
    /**
     * Has the last expected frame come?
     */
    private boolean finished;

    //private static final byte[] END = new String("END").getBytes();
    private String host;
    private int port;
    private byte[] queueName;
    //private BlockingQueue<byte[]> dataQueue = new ArrayBlockingQueue<>(10000);
    private Jedis jedis;

    /**
     * Creates a producer expecting frames in range [firstFrameId, lastFrameId), with an additional parameter qSize
     */
    public RedisSimpleStreamProducer(String host, int port, String queueName) {

        stream = new PriorityQueue<>();
        this.host = host;
        this.port = port;
        this.queueName = queueName.getBytes();
        finished = false;
        jedis = new Jedis(host, port);

        System.out.println("Check_init_RedisSimpleStreamProducer, " + System.currentTimeMillis() +
                ", host: " + this.host + ", port: " + this.port + ", qName: " + this.queueName);
    }

    /**
     * Add frame to the queue if it is fully processed
     */
    public void addFrame(GeneralizedStreamFrame streamFrame) {
        synchronized (stream) {
            stream.add(streamFrame);
        }
    }

    /**
     * Get expected frame from the queue.
     *
     * @return next expected frame, or null if it has not come yet.
     */
    public GeneralizedStreamFrame pollFrame() {
        synchronized (stream) {
            return stream.poll();
        }
    }

    public GeneralizedStreamFrame getPeekFrame() {
        synchronized (stream) {
            return stream.isEmpty() ? null : stream.peek();
        }
    }

    public int getStreamSize() {
        synchronized (stream) {
            return stream.size();
        }
    }

    @Override
    public void run() {
        while (!finished) {
            try {
                GeneralizedStreamFrame peekFrame = getPeekFrame();
                if (peekFrame == null) {
                    //System.out.println("peekFrame == null");
                    Thread.sleep(10);
                } else {

                    //System.out.println("peekFrame.frameId (" + peekFrame.frameId +") == 1 + currentFrameID: " + currentFrameID);
                    GeneralizedStreamFrame nextFrame = pollFrame();
                    List<float[]> data = (List<float[]>) nextFrame.data;
                    jedis.rpush(this.queueName, helperFunctions.toBytes(data));

                    System.out.println("finishedAdd: " + System.currentTimeMillis() + ",Fid: " + nextFrame.frameId);
                }

            } catch (Exception e) {
                System.out.print("Exception: ");
                e.printStackTrace();
            }
        }
    }
}
