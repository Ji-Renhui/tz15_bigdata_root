package tz15_bigdata_root.flume.service;

import org.apache.log4j.Logger;
import tz15_bigdata_root.common.config.ConfigUtil;
import tz15_bigdata_root.flume.constant.ConstantFields;
import tz15_bigdata_root.flume.constant.ErrorMapFields;
import tz15_bigdata_root.flume.interceptor.ELKInterceptor;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * @author: KING
 * @description: 数据清洗类
 * @Date:Created in 2020-03-02 21:56
 */
public class DataCheck {
    private static final Logger LOG = Logger.getLogger(ELKInterceptor.class);
    static Properties properties;
    static {
        String filePath = "common/datatype.properties";
        properties = ConfigUtil.getInstance().getProperties(filePath);
    }


    /**
     * 数据解析并且校验
     * @return
     */
    public static Map txtParseAndValidation(String filename,String absolute_filename,String line){

        Map data = new HashMap<String,String>(); //存放解析数据的map

        //TODO 异常数据需要进行存储，查询，分析   用于异常数据监控，提升数据质量
        Map errorMap = new HashMap<String,String>(); //存放异常数据的map

        //数据清洗，转换，加工
        //定义一个数据字典
        //imei,imsi,longitude,latitude,phone_mac,device_mac,device_number,collect_time,username,phone,object_username,send_message,accept_message,message_time
        //000000000000000,000000000000000,24.000000,25.000000,aa-aa-aa-aa-aa-aa,bb-bb-bb-bb-bb-bb	32109231	1257305985	andiy	18609765435	judy			1789098763
        //获取数据类型
        String table = filename.split("_")[0].toLowerCase();
        //根据数据类型获取 数据字典
        String[] fields = properties.get(table).toString().split(",");//字典数组
        String[] lines = line.split("\t");
        if(fields.length == lines.length){
            //TODO 字段和值映射
            for (int i = 0; i < fields.length; i++) {
                data.put(fields[i],lines[i]);
            }
            //TODO 数据加工  主要是为了满足后续的业务需求
            //1.没有唯一ID，可以解决数据重复消费问题
            data.put("id", UUID.randomUUID().toString().replace("-",""));
            //这个数据最终要进入ES，需求，要根据表来进行查询，要查询文件最终存放的目录。
            //文件名，文件绝对路径会丢失，信息丢失掉了
            data.put("table",table);
            data.put("rksj",System.currentTimeMillis()/1000+"");
            data.put(ConstantFields.FILE_NAME,filename);
            data.put(ConstantFields.ABSOLUTE_FILENAME,absolute_filename);
        }else{
            errorMap.put(ErrorMapFields.LENGTH,"字段和值的元素个数不匹配");
            errorMap.put(ErrorMapFields.LENGTH_ERROR,ErrorMapFields.LENGTH_ERROR_NUM);
        }

        //TODO 数据清洗
        if(data!=null && data.size()>0){
            //数据校验，清洗，搜集异常数据错误信息
           errorMap = DataValidation.dataValidation(data);
        }

        if(errorMap.size()>0){
            //如果数据有错误，那么data中数据有问题，将data置为空，不推送到kafka
            data = null;
            //错误数据写入ES
            //TODO 调用ES接口写入到ES，方便后续查错
        }
        return data;
    }
}
