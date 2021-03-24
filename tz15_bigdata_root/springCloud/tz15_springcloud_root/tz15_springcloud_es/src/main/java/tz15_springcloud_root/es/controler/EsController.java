package tz15_springcloud_root.es.controler;


import io.swagger.annotations.Api;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import tz15_springcloud_root.es.feign.HbaseFeign;
import tz15_springcloud_root.es.service.EsService;

import javax.annotation.Resource;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-21 21:56
 */
@Controller //控制器，转发器
@RequestMapping("/es")
@Api(value = "ES查询")
public class EsController {


    @Resource
    private HbaseFeign hbaseFeign;
    @Resource
    private EsService esService;


    @ResponseBody
    @RequestMapping(value = "/getAllLocus",method = {RequestMethod.POST})
    //传入经纬度，时间，索引名
    public List<Map<String,Object>> getAllLocus(@RequestParam(name="table") String table,
                                                @RequestParam(name="rowkey") String rowkey){
        //查询一对多的关系，需要查hbase,引入feign
        Set<String> macs = hbaseFeign.getRowkeys(table, rowkey);
        Iterator<String> iterator = macs.iterator();
        String phone_mac = iterator.next();
        //根据MAC查询轨迹
        return esService.getAllLocus(phone_mac);
    }
}
