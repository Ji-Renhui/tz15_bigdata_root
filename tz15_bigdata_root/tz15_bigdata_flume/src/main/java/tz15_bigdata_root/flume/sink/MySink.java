package tz15_bigdata_root.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;
import tz15_bigdata_root.flume.constant.FlumeParamConstant;
import tz15_bigdata_root.kafka.producer.StringProducer;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-02-22 22:16
 */
public class MySink extends AbstractSink implements Configurable {

    private static final Logger LOG = Logger.getLogger(MySink.class);

    private String topic;
    @Override
    public void configure(Context context) {
        topic = context.getString(FlumeParamConstant.TOPIC_NAME);
        LOG.error("topic=======" + topic);
    }

    @Override
    public void start() {
    }

    @Override
    public void stop () {
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        // Start transaction
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            Event event = ch.take();
            if(event == null){
                txn.rollback();
                return Status.BACKOFF;
            }
            String line = new String(event.getBody());
            try {
                StringProducer.producer(topic,line);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("成功推送数据"+topic+"=="+line);
            //TODO 推送到kafka
            //Producer.send("");
            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();
            status = Status.BACKOFF;
            if (t instanceof Error) {
                throw (Error)t;
            }
        }finally {
            txn.close();
        }
        return status;
    }
}
