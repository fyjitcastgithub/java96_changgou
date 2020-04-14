package com.changgou.seckill.service.impl;

import com.changgou.seckill.pojo.SeckillGoods;
import com.changgou.seckill.service.SecKillGoodsService;
import com.sun.org.apache.xpath.internal.operations.Equals;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SecKillGoodsServiceImpl implements SecKillGoodsService {

    @Autowired
    private RedisTemplate redisTemplate;

    public static final String SECKILL_GOODS_KEY="seckill_goods_";

    public static final String SECKILL_GOODS_STOCK_COUNT_KEY="seckill_goods_stock_count_";

    @Override
    public List<SeckillGoods> list(String time) {
        List<SeckillGoods> list = redisTemplate.boundHashOps(SECKILL_GOODS_KEY + time).values();

        //更新库存数据的来源
        for (SeckillGoods seckillGoods : list) {
            if(SECKILL_GOODS_STOCK_COUNT_KEY+seckillGoods.getId()!=null||
                    (SECKILL_GOODS_STOCK_COUNT_KEY+seckillGoods.getId()).equals("")){
                System.out.println("SECKILL_GOODS_STOCK_COUNT_KEY+seckillGoods.getId()"+SECKILL_GOODS_STOCK_COUNT_KEY+seckillGoods.getId());
                String value = (String) redisTemplate.opsForValue().get(SECKILL_GOODS_STOCK_COUNT_KEY+seckillGoods.getId());
                seckillGoods.setStockCount(Integer.parseInt(value));
            }

        }
        return list;
    }
}
