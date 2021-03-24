package tz15_springcloud_root.es.service;

import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;
import tz14_bigdata_root.es.jest.jestservice.JestService;
import tz14_bigdata_root.es.jest.jestservice.ResultParse;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-21 22:17
 */
@Service
public class LocusService {

    @Resource
    private LocusService locusService;

    public static void main(String[] args) {
        LocusService locusService = new LocusService();
        SearchResult locusMAC = locusService.getLocusMAC("wechat_20091106", "wechat_20091106", "24_25", "1257440461");
        Map<String, Object> stringObjectMap = ResultParse.parseSearchResult(locusMAC);
        System.out.println(stringObjectMap.toString());
    }


    /**
     * 轨迹碰撞查询
     * @param indexName
     * @param typeName
     * @param locusPointNew
     * @return
     */
    public Set<String> getLocus(String indexName,String typeName,Map<String,String> locusPointNew){

        int n = 0;
        //存放最终的交集结果
        Set set = new HashSet();
        //查询索引表，获取所有的主表主键
        //针对ES查询  使用jest查询
        //对所有的点进行遍历
        Iterator<String> iterator = locusPointNew.keySet().iterator();
        while (iterator.hasNext()){
            String point = iterator.next();
            String time = locusPointNew.get(point);
           // LocusService LocusService = new LocusService;
            SearchResult locusMAC = locusService.getLocusMAC(indexName, typeName, point, time);
            Map<String, Object> stringObjectMap = ResultParse.parseSearchResult(locusMAC);
            List<Map<String, String>> list = (List<Map<String, String>>) stringObjectMap.get("data");

            //如果N>=2 说明第一次查询为空，整个交集为空，直接返回。
            if(n>=2){
               return new HashSet<>();
            }else if(set.size()>0){
                    //如果set大于0，说明Set之前已经有别的轨迹点数据了，(后面全部取交集)
                    Set<String> set1 = new HashSet<>();
                    list.forEach(map->{
                        String phone_mac = map.get("phone_mac");
                        set1.add(phone_mac);
                    });
                //取交集
                set.retainAll(set1);
            }else{
                n = n + 1;
                //如果set==0,初始化SET，将第一个点附近的MAC放到set里面去(第一次取并集)
                list.forEach(map->{
                    String phone_mac = map.get("phone_mac");
                    set.add(phone_mac);
                });
            }
        }
        return set;
    }


    /**
     *
     * @param indexName
     * @param typeName
     * @param point       空间参数
     * @param time         时间参数
     * @return
     */
    private SearchResult getLocusMAC(String indexName, String typeName, String point, String time){

        //point = 24_25
        //1323243543
        SearchResult searchResult = null;
        //获取ES客户端连接
        JestClient jestClient = JestService.getJestClient();

        //获取经纬度信息
        String[] split = point.split("_");
        String longitude = split[0];
        String latitude = split[1];

        long disTime = 120L;
        long currentTime = Long.valueOf(time);
        long beginTime = currentTime - disTime; //往前推2分钟
        long endTime = currentTime + disTime;   //往后推2分钟

        //构造ES查询

        //查询构造器
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(new String[]{"phone_mac"},new String[0]);

        //bool查询器
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.rangeQuery("collect_time").gte(beginTime).lte(endTime));
        boolQueryBuilder.filter(QueryBuilders.geoDistanceQuery("location").point(Double.valueOf(longitude),Double.valueOf(latitude)).distance("100",DistanceUnit.METERS));

        //组装searchSourceBuilder
        searchSourceBuilder.query(boolQueryBuilder);
        //使用构造器构造一个查询器
        System.out.println(searchSourceBuilder.toString());
        Search.Builder builder = new Search.Builder(searchSourceBuilder.toString());

        //设置indexName,typeName
        builder.addIndex(indexName);
        builder.addType(typeName);

        Search build = builder.build();

        try {
            searchResult = jestClient.execute(build);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            JestService.closeJestClient(jestClient);
        }
        return searchResult;
    }



}
