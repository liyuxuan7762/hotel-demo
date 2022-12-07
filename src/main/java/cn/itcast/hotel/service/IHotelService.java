package cn.itcast.hotel.service;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.SearchParam;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;
import java.util.Map;

public interface IHotelService extends IService<Hotel> {
    PageResult search(SearchParam searchParam);

    Map<String, List<String>> filter(SearchParam searchParam);

    List<String> suggest(String key);

    void deleteById(Long id);

    void insertById(Long id);
}
