package tz15_springcloud_root.es;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;
import org.springframework.cloud.openfeign.EnableFeignClients;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-20 22:26
 */
@SpringBootApplication
@EnableEurekaServer
@EnableFeignClients //开启feign
public class EsApplication {
    public static void main(String[] args) {
        SpringApplication.run(EsApplication.class,args);
    }
}
