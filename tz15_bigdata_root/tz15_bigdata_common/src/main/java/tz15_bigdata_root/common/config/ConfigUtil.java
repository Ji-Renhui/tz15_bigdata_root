package tz15_bigdata_root.common.config;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author robot
 * @ClassName ConfigUtil
 * @Description
 * @Date 2021/1/26 13:31
 * @Created by robot
 **/
public class ConfigUtil {
    private static final Logger LOG = Logger.getLogger(ConfigUtil.class);

    //私有静态变量
    private static volatile ConfigUtil configUtil;
    //私有构造方法
    private ConfigUtil(){}
    //对外访问接口
    public static ConfigUtil getInstance() {

        if (configUtil==null){
            synchronized (ConfigUtil.class){
                 if(configUtil==null){
                      configUtil=new ConfigUtil();
                }
            }
        }
        return configUtil;
    }

    public  Properties getProperties(String path){

        Properties properties = new Properties();
        try {
            InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream(path);
            properties.load(resourceAsStream);
        } catch (IOException e) {
            LOG.error("配置文件读取失败",e);
            e.printStackTrace();
        }
        return properties;
    }

    public static void main(String[] args) {
        String path = "kafka/kafka-server-config.properties";
        Properties properties = ConfigUtil.getInstance().getProperties(path);
        properties.keySet().forEach(key -> {
            System.out.println(key);
            System.out.println(properties.getProperty(key.toString()));
        });
    }
}