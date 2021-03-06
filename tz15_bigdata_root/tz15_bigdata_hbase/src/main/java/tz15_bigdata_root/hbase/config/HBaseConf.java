package tz15_bigdata_root.hbase.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;
import tz15_bigdata_root.hbase.insert.HBaseInsertHelper;
import tz15_bigdata_root.hbase.split.SplitRegionUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * @author:
 * @description:  读取HBASE配置文件  获取连接
 * @Date:Created in 2019-03-29 22:15
 */
public class HBaseConf implements Serializable {
    //读取HBASE配置文件
    //获取连接
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(HBaseConf.class);
    //配置路径
    private static final String HBASE_SITE = "hbase/hbase-site.xml";



    private volatile static HBaseConf hbaseConf;
    //hbase 配置文件
    private Configuration configuration;
    //hbase 连接
    private volatile Connection conn;

    /**
     * 初始化HBaseConf的时候加载配置文件
     */
    private HBaseConf() {
     getHconnection();

    }

    /**
     * 单例 初始化HBaseConf
     * @return
     */
    public static HBaseConf getInstance() {
        if (hbaseConf == null) {
            synchronized (HBaseConf.class) {
                if (hbaseConf == null) {
                    hbaseConf = new HBaseConf();
                }
            }
        }
        System.out.println("=======hbaseConf=:===="+hbaseConf);
        return hbaseConf;
    }


    //获取连接
    public Configuration getConfiguration(){
        if(configuration==null){
            configuration = HBaseConfiguration.create();
            //通过addResource方法把hbase配置文件加载进来
            configuration.addResource(HBASE_SITE);



            LOG.info("加载配置文件" + HBASE_SITE+ "成功");
        }
        return configuration;
    }

    public BufferedMutator getBufferedMutator(String tableName) throws IOException {
        return getHconnection().getBufferedMutator(TableName.valueOf(tableName));
    }


    public Connection getHconnection(){

        if(conn==null){
            //加载配置文件
       configuration = getConfiguration();
            synchronized (HBaseConf.class) {
                if (conn == null) {
                    try {
                        conn = ConnectionFactory.createConnection(configuration);
                    } catch (IOException e) {
                        LOG.error(String.format("获取hbase的连接失败  参数为： %s", toString()), e);
                    }
                }
            }
        }
        System.out.println(conn+"-----conn-------");
        return conn;
    }


    public static void main(String[] args) throws Exception {
        String hbase_table = "test:aaaa";
        HBaseTableUtil.createTable(hbase_table, "cf", true, -1, 1, SplitRegionUtil.getSplitKeysBydinct());



//
//        Put put = new Put("01".getBytes());
//        put.add("cf".getBytes(),"qual".getBytes(),"111111".getBytes());
//        //put 'test:aaaa','1001','cf:qual','11111'
//
//        //'test:aaaa','1001','info:age','28'
//        HBaseInsertHelper.put(hbase_table,put);
//        System.out.println("运行成功");


    }
}
