package tz15_bigdata_root.es.client;

import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import tz15_bigdata_root.common.config.ConfigUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-07 22:33
 */
public class ESclientUtil {
    private static final Logger LOG = Logger.getLogger(ESclientUtil.class);

    private volatile static TransportClient client;

    //配置文件路径

    private static final String ES_CONFIG_PATH = "es/es_cluster.properties";
    private static Properties properties;

    static {
        properties = ConfigUtil.getInstance().getProperties(ES_CONFIG_PATH);
    }

    private ESclientUtil(){}

    public static TransportClient getClient(){

            //解决netty冲突
        TransportClient client = null;
        try {
            System.setProperty("es.set.netty.runtime.available.processors", "false");

            String host1 = properties.get("es.cluster.nodes1").toString();
            String host2 = properties.get("es.cluster.nodes2").toString();
            String host3 = properties.get("es.cluster.nodes3").toString();

            String cluster_name = properties.get("es.cluster.name").toString();
            Integer tcp_port = Integer.valueOf(properties.get("es.cluster.tcp.port").toString());

            //ES配置
            Settings settings = Settings.builder()
                    .put("cluster.name", cluster_name).build();
            //创建客户端
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host1), 9300))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host2), 9300))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host3), 9300));
        } catch (UnknownHostException e) {
            LOG.error("索引创建失败",e);
        }

// on shutdown

         //   client.close();



        return client;
    }

}
