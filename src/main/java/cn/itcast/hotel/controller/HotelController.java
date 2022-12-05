package cn.itcast.hotel.controller;

import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.SearchParam;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/hotel")
public class HotelController {

    @Resource
    IHotelService hotelService;

    @PostMapping("/list")
    public PageResult search(@RequestBody SearchParam searchParam) {
        return hotelService.search(searchParam);
    }

    @PostMapping("/filters")
    public Map<String, List<String>> filter(@RequestBody SearchParam searchParam) {
        return hotelService.filter(searchParam);
    }

    @GetMapping("/suggestion")
    public List<String> filter(String key) {
        return hotelService.suggest(key);
    }
}



