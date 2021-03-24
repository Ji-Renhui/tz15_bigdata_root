package tz15_bigdata_root.flume.source;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;
import tz15_bigdata_root.flume.constant.ConstantFields;
import tz15_bigdata_root.flume.constant.FlumeParamConstant;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.io.File.separator;


/**
 * @ClassName Source
 * @Description
 * @Date 2021/1/25 17:18
 * @Created by robot
 **/
public class Source extends AbstractSource implements Configurable, PollableSource {
    private static final Logger LOG=Logger.getLogger(Source.class);
    String foldDir;
    String succDir;
    String errorDir;
    private int fileNum;
    private List<File> listFiles;
    private List<Event> listEvents;
    @Override
    public void configure(Context context) {

        listEvents = new ArrayList<>();
        foldDir=context.getString(FlumeParamConstant.FOLD_DIR);
        succDir=context.getString(FlumeParamConstant.SUCC_DIR);
        errorDir=context.getString(FlumeParamConstant.ERROR_DIR);
        fileNum=context.getInteger(FlumeParamConstant.FILE_NUM);
        LOG.error("获取folderDir" + foldDir);
        LOG.error("获取succDir" + succDir);
        LOG.error("获取errorDir" + errorDir);
        LOG.error("获取fileNum" + fileNum);
    }
    @Override
    public Status process() throws EventDeliveryException {

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Status status = null;
        System.out.println("====1===source Process开始执行");
        try {

            List<File> files = (List<File>) FileUtils.listFiles(new File(foldDir), new String[]{"txt"}, true);
                int fileCount=files.size();
            if (fileCount > fileNum){
                listFiles=files.subList(0,fileNum);
            }else {
                listFiles=files;
            }
            if(listFiles.size()>0){
                for (int i = 0; i < listFiles.size(); i++) {
                    File file=listFiles.get(i);
                    String filename= file.getName();

                    String succDirN=succDir + separator+"20210125";
                    String errorDirNew = errorDir + separator + "20210125";
                    String absouluteFileName=succDirN+filename;
                    try {
                        if(new File(absouluteFileName).exists()) {
                            //不处理
                        }else {
                            List<String> lines = FileUtils.readLines(file);
                            lines.forEach(line->{
                                Event e = new SimpleEvent();

                                e.setBody(line.getBytes());
                                Map headers=new HashMap<String,String>();
                                headers.put(ConstantFields.FILE_NAME,filename);
                                headers.put(ConstantFields.ABSOLUTE_FILENAME,absouluteFileName);
                                e.setHeaders(headers);
                                LOG.error("数据内容为:  "+line);
                                System.out.println("数据内容为:  "+line);
                                listEvents.add(e);

                            });
                            //备份
                            FileUtils.moveToDirectory(file,new File(succDirN),true);
                            System.out.println("====2===备份成功");
                        }

                    } catch (IOException e) {
                        FileUtils.moveToDirectory(file,new File(errorDirNew),true);
                        LOG.error(null,e);
                    }
                    //批量推送  需要一个list<Event>
                    getChannelProcessor().processEventBatch(listEvents);
                    listEvents.clear();
                }
                status=Status.READY;
            }
        } catch (Exception e) {
            status=Status.BACKOFF;
            e.printStackTrace();

        }
        return status;
    }


    @Override
    public void start() {
        // Initialize the connection to the external client
    }

    @Override
    public void stop () {
        // Disconnect from external client and do any additional cleanup
        // (e.g. releasing resources or nulling-out field values) ..
    }



    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}
