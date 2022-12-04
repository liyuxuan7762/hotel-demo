package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

@SpringBootTest
public class HotelDocDemoApplicationTest {

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

    // 插入一条酒店数据
    @Test
    void addDoc() throws IOException {
        Hotel hotel = hotelService.getById(38665L);
        HotelDoc hotelDoc = new HotelDoc(hotel);

        IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);

        this.client.index(request, RequestOptions.DEFAULT);
    }

    // 查询一条文档

    @Test
    void queryDoc() throws IOException {
        GetRequest request = new GetRequest("hotel", "38665");
        GetResponse response = this.client.get(request, RequestOptions.DEFAULT);
        String json = response.getSourceAsString();
        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        System.out.println(hotelDoc);
    }

    @Test
    void updateDoc() throws IOException {
        UpdateRequest request = new UpdateRequest("hotel", "38665");
        HashMap<String, Object> map = new HashMap<>();
        map.put("price", 999);
        request.doc(map);
        this.client.update(request, RequestOptions.DEFAULT);
    }

    @Test
    void deleteDoc() throws IOException {
        DeleteRequest request = new DeleteRequest("hotel", "38665");
        this.client.delete(request, RequestOptions.DEFAULT);
    }

    // 批量导入酒店数据

    @Test
    void importHotelInfo() throws IOException {
        List<Hotel> list = hotelService.list();
        BulkRequest request = new BulkRequest();
        int i = 0;
        for (Hotel hotel : list) {
            i++;
            HotelDoc hotelDoc = new HotelDoc(hotel);
            request.add(
                    new IndexRequest("hotel")
                            .id(hotelDoc.getId().toString()).
                            source(JSON.toJSONString(hotelDoc), XContentType.JSON)
            );
        }

        this.client.bulk(request, RequestOptions.DEFAULT);
    }
}
