package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.SearchParam;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Resource
    private RestHighLevelClient client;

    @Override
    public PageResult search(SearchParam searchParam) {
        try {
            SearchRequest request = new SearchRequest("hotel");
            // 查询关键字
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
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            return parseResult(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
