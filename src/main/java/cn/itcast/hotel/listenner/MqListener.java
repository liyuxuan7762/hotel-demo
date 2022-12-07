package cn.itcast.hotel.listenner;

import cn.itcast.hotel.constants.MqConstants;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
public class MqListener {
    @Resource
    private IHotelService service;

    @RabbitListener(queues = MqConstants.HOTEL_INSERT_QUEUE)
    public void insertListener(Long id) {
        service.insertById(id);
    }
    @RabbitListener(queues = MqConstants.HOTEL_DELETE_QUEUE)
    public void deleteListener(Long id) {
        service.deleteById(id);
    }
}
