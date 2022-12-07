package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.SearchParam;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Resource
    private RestHighLevelClient client;

    @Override
    public PageResult search(SearchParam searchParam) {
        try {
            SearchRequest request = new SearchRequest("hotel");
            // 查询关键字
            buildBasicQuery(searchParam, request);
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            return parseResult(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void buildBasicQuery(SearchParam searchParam, SearchRequest request) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        String key = searchParam.getKey();
        if (key == null || "".equals(key)) {
            boolQueryBuilder.must(QueryBuilders.matchAllQuery());
        } else {
            boolQueryBuilder.must(QueryBuilders.matchQuery("all", key));
        }
        if (searchParam.getCity() != null && !"".equals(searchParam.getCity())) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("city", searchParam.getCity()));
        }
        if (searchParam.getBrand() != null && !"".equals(searchParam.getBrand())) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("brand", searchParam.getBrand()));
        }
        if (searchParam.getStarName() != null && !"".equals(searchParam.getStarName())) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("startName", searchParam.getStarName()));
        }
        if (searchParam.getMaxPrice() != null) {
            boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").lt(searchParam.getMaxPrice()));
        }
        if (searchParam.getMinPrice() != null) {
            boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gt(searchParam.getMinPrice()));
        }

        if (!StringUtils.isEmpty(searchParam.getLocation())) {
            // 按照距离排序
            request.source().sort(SortBuilders.geoDistanceSort(
                                    "location",
                                    new GeoPoint(searchParam.getLocation())
                            )
                            .order(SortOrder.ASC)
                            .unit(DistanceUnit.KILOMETERS)
            );
        }


        // 设置广告置顶
        FunctionScoreQueryBuilder functionScoreQuery =
                QueryBuilders.functionScoreQuery(
                        // 原始查询，相关性算分的查询
                        boolQueryBuilder,
                        // function score的数组
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                // 其中的一个function score 元素
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        // 过滤条件
                                        QueryBuilders.termQuery("isAD", true),
                                        // 算分函数
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });

        // 分页
        int page = searchParam.getPage();
        int size = searchParam.getSize();
        request.source().from((page - 1) * size);
        request.source().size(size);
        request.source().query(functionScoreQuery);
    }

    /**
     * 根据星级，品牌，和城市对酒店进行聚合分析
     *
     * @return
     */
    @Override
    public Map<String, List<String>> filter(SearchParam searchParam) {
        try {
            Map<String, List<String>> map = new HashMap<>();
            SearchRequest request = new SearchRequest("hotel");

            buildBasicQuery(searchParam, request);

            request.source().size(0);
            request.source().aggregation(
                    AggregationBuilders.terms("brandAgg")
                            .field("brand")
                            .size(30)
            );
            request.source().aggregation(
                    AggregationBuilders.terms("cityAgg")
                            .field("city")
                            .size(30)
            );
            request.source().aggregation(
                    AggregationBuilders.terms("starNameAgg")
                            .field("starName")
                            .size(30)
            );

            SearchResponse search = this.client.search(request, RequestOptions.DEFAULT);

            List<String> name = parseRes(search, "brandAgg");
            map.put("brand", name);

            name = parseRes(search, "cityAgg");
            map.put("city", name);

            name = parseRes(search, "starNameAgg");
            map.put("starName", name);

            return map;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> suggest(String key) {
        try {
            List<String> list = new ArrayList<>();
            SearchRequest request = new SearchRequest("hotel");
            request.source().suggest(new SuggestBuilder().addSuggestion(
                    "search_suggestion",
                    SuggestBuilders.completionSuggestion("suggestion")
                            .prefix(key)
                            .skipDuplicates(true)
                            .size(10)
            ));
            SearchResponse response = this.client.search(request, RequestOptions.DEFAULT);

            CompletionSuggestion suggestion = response.getSuggest().getSuggestion("search_suggestion");

            if (suggestion.getOptions().size() > 0) {
                for (CompletionSuggestion.Entry.Option option : suggestion.getOptions()) {
                    String s = option.getText().toString();
                    list.add(s);
                }
            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteById(Long id) {
        try {
            DeleteRequest request = new DeleteRequest("hotel", id.toString());
            this.client.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insertById(Long id) {
        try {
            Hotel hotel = this.getById(id);
            HotelDoc hotelDoc = new HotelDoc(hotel);
            IndexRequest request = new IndexRequest("hotel").id(id.toString());
            request.source(JSON.toJSONString(hotel), XContentType.JSON);
            this.client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> parseRes(SearchResponse search, String aggName) {
        Aggregations aggregations = search.getAggregations();
        Terms term = aggregations.get(aggName);
        List<String> list = new ArrayList<>();
        List<? extends Terms.Bucket> buckets = term.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            String name = bucket.getKeyAsString();
            list.add(name);
        }

        return list;
    }

    private PageResult parseResult(SearchResponse response) {
        SearchHits searchHits = response.getHits();
        long total = searchHits.getTotalHits().value;
        SearchHit[] hits = searchHits.getHits();
        List<HotelDoc> hotels = new ArrayList<>();
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length != 0) {
                hotelDoc.setDistance(sortValues[0]);
            }
            hotels.add(hotelDoc);
        }
        return new PageResult(total, hotels);
    }
}
