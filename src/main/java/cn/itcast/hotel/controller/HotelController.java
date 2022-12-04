package cn.itcast.hotel.controller;

import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.SearchParam;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController
@RequestMapping("/hotel")
public class HotelController {

    @Resource
    IHotelService hotelService;

    @PostMapping("/list")
    public PageResult search(@RequestBody SearchParam searchParam) {
        return hotelService.search(searchParam);
    }
}
