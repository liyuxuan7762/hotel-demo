package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Map;

public class HotelSearchDemoApplicationTest {

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

    // matchAll
    @Test
    void testMatchAll() throws IOException {
        // 1.创建查询请求
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchAllQuery()).size(1000);
        // 2.发送查询请求
        SearchResponse response = this.client.search(request, RequestOptions.DEFAULT);
        // 3.解析查询请求
        parseResult(response);
    }

    // 查询如家小于400元的31.21，121.5这个位置10km内的酒店 bool查询
    @Test
    void testBool() throws IOException {
        // 1.创建查询请求
        SearchRequest request = new SearchRequest("hotel");
        // 查询如家小于400元的31.21，121.5这个位置10km内的酒店
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.termQuery("brand", "如家"));
        boolQuery.mustNot(QueryBuilders.rangeQuery("price").gt(400));
        boolQuery.filter(QueryBuilders.geoDistanceQuery("location").point(31.21, 121.5).distance(10, DistanceUnit.KILOMETERS));

        request.source().query(boolQuery);
        // 2.发送查询请求
        SearchResponse response = this.client.search(request, RequestOptions.DEFAULT);
        // 3.解析查询请求
        parseResult(response);
    }

    // 测试分页和排序
    @Test
    void testOrderAndPagination() throws IOException {
        int pageNo = 1, pageSize = 5;
        // 1.创建查询请求
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchAllQuery());
        request.source().from((pageNo - 1) * pageSize).size(pageSize);
        request.source().sort("price", SortOrder.ASC);
        // 2.发送查询请求
        SearchResponse response = this.client.search(request, RequestOptions.DEFAULT);
        // 3.解析查询请求
        parseResult(response);
    }

    @Test
    void testHeightLight() throws IOException {
        int pageNo = 1, pageSize = 5;
        // 1.创建查询请求
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchQuery("name", "如家"));
        request.source().from((pageNo - 1) * pageSize).size(pageSize);
        request.source().sort("price", SortOrder.ASC);
        request.source().highlighter(new HighlightBuilder().field("name"));
        // 2.发送查询请求
        SearchResponse response = this.client.search(request, RequestOptions.DEFAULT);
        // 3.解析查询请求
        parseResult(response);
    }

    private static void parseResult(SearchResponse response) {
        SearchHits searchHits = response.getHits();
        long total = searchHits.getTotalHits().value;
        System.out.println("共查询到" + total + "条记录");
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (!CollectionUtils.isEmpty(highlightFields)) {
                // 如果需要高亮显示
                HighlightField highlightField = highlightFields.get("name");
                if (highlightField != null) {
                    String name = highlightField.getFragments()[0].toString();
                    hotelDoc.setName(name);

                }
            }
            System.out.println(hotelDoc);
        }
    }


    // 实现自动补全功能
    @Test
    public void testAutoFill() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().suggest(new SuggestBuilder().addSuggestion(
                "search_suggestion",
                SuggestBuilders.completionSuggestion("suggestion")
                        .prefix("s")
                        .skipDuplicates(true)
                        .size(10)
        ));
        SearchResponse response = this.client.search(request, RequestOptions.DEFAULT);

        CompletionSuggestion suggestion = response.getSuggest().getSuggestion("search_suggestion");

        for (CompletionSuggestion.Entry.Option option : suggestion.getOptions()) {
            String s = option.getText().toString();
            System.out.println(s);
        }
    }
}
