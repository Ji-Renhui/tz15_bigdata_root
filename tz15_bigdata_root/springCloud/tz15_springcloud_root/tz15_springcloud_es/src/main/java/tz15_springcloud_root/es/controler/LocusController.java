package tz15_springcloud_root.es.controler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import tz15_springcloud_root.es.service.LocusService;

import java.util.Map;
import java.util.Set;

/**
 * @author: KING
 * @description: 轨迹碰撞
 * @Date:Created in 2020-03-21 22:16
 */
@Controller //控制器，转发器
@RequestMapping("/locus")
@Api(value = "轨迹碰撞 轨迹伴随查询")
public class LocusController {

    @Autowired
    private LocusService locusService;

    @ApiOperation(value = "轨迹碰撞查询",notes = "轨迹碰撞查询")
    @ApiImplicitParams({
            @ApiImplicitParam(name ="locusPoint",value = "组合查询条件",required = true,dataType = "string")
    })
    @ResponseBody
    @RequestMapping(value = "/getLocus",method = {RequestMethod.GET,RequestMethod.POST})
    public Set<String> getLocus(@RequestParam(name="locusPoint") String locusPoint){
       /* {
            "indexName":"",
                "typeName":"",
                "locusPoint":"{
                    "lon_lat":"时间戳",
                    "lon1_lat1":"时间戳1"
                }"
        }*/
       //首先解析查询条件
        Map<String, String> params = JSON.parseObject(locusPoint, new TypeReference<Map<String, String>>() {
        });
        String indexName = params.get("indexName");
        String typeName = params.get("typeName");
        Map<String, String> locusPointNew = JSON.parseObject(params.get("locusPoint"), new TypeReference<Map<String, String>>() {});
        //查询索引表，获取所有的主表主键
        return locusService.getLocus(indexName,typeName,locusPointNew);
    }

}
