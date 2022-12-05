package cn.itcast.hotel;

import cn.itcast.hotel.service.IHotelService;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@SpringBootTest
public class HotelAggregationDemoApplicationTest {

    private RestHighLevelClient client;

    @Resource
    private IHotelService hotelService;


    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://95.179.138.182:9200")
        ));
    }

    @AfterEach
    void close() throws IOException {
        this.client.close();
    }


    /**
     * 根据酒店品牌数量聚类
     * @throws IOException
     */
    @Test
    public void testAgg() throws IOException {
        // 创建请求
        SearchRequest request = new SearchRequest("hotel");
        // 创建DSL
        request.source().size(0);
        request.source().aggregation(
                AggregationBuilders.terms("brandAgg")
                        .field("brand")
                        .size(30)
        );
        // 发送请求
        SearchResponse search = this.client.search(request, RequestOptions.DEFAULT);
        // 解析请求
        Aggregations aggregations = search.getAggregations();
        Terms terms = aggregations.get("brandAgg");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            String brandName = bucket.getKeyAsString();
            System.out.println(brandName);
        }
    }



    @Test
    public void testHotelAgg() {
        // Map<String, List<String>> filter = hotelService.filter();
        // System.out.println(filter);
    }


}
