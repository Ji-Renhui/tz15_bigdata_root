package tz15_springcloud_root.es.feign;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Set;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-23 21:53
 */

@FeignClient(name = "tz14-springcloud-hbase")
public interface HbaseFeign {
    @ResponseBody
    @RequestMapping(value = "/hbase/getRowkeys",method = {RequestMethod.POST})
    Set<String> getRowkeys(@RequestParam(name = "table") String table,
                           @RequestParam(name = "rowkey") String rowkey);
}

