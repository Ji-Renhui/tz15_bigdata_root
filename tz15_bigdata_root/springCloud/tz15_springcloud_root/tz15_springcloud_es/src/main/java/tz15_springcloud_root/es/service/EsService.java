package tz15_springcloud_root.es.service;

import io.searchbox.client.JestClient;
import io.searchbox.core.SearchResult;
import org.springframework.stereotype.Service;
import tz14_bigdata_root.es.jest.jestservice.JestService;
import tz14_bigdata_root.es.jest.jestservice.ResultParse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-21 21:56
 */
@Service
public class EsService {


    public List<Map<String,Object>> getAllLocus(String mac){
        List<Map<String,Object>> list = new ArrayList<>();
        JestClient jestClient = null;

        try {
            jestClient = JestService.getJestClient();
            //定义返回字段 "_source"
            String[] includes = new String[]{"longitude","latitude","collect_time"};
            /*JestClient jestClient,
            String indexName,
            String typeName,
            String field,
            String fieldValue,
            String sortField,
            String sortValue,
            int pageNumber,
            int pageSize*/
            SearchResult search = JestService.search(jestClient,
                    "",
                    "",
                    "phone_mac.keyword",
                    mac,
                    "collect_time",
                    "asc",
                    1,
                    1000,
                    includes);
            list = ResultParse.parseSearchResultOnly(search);

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            JestService.closeJestClient(jestClient);
        }

        return list;
    }
}
