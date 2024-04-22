package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import io.lettuce.core.RedisClient;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Autowired
    private ISeckillVoucherService seckillVoucherService;
    @Autowired
    private RedisIdWorker redisIdWorker;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
   @Autowired
   private RedissonClient redissonClient;
   //成员变量的代理对象
   private IVoucherOrderService proxy;

 /*
 @Override
    public Result seckillVoucher(Long voucherId) throws InterruptedException {
        // 1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);

        LocalDateTime beginTime = voucher.getBeginTime();
        if(LocalDateTime.now().isBefore(beginTime)){
            return Result.fail("活动尚未开始");
        }

        // 3.判断秒杀是否已经结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀已经结束！");
        }
        // 4.判断库存是否充足
        if (voucher.getStock() < 1) {
            // 库存不足
            return Result.fail("库存不足！");
        }
        //事务想要生效，还得利用代理来生效
        Long userId = UserHolder.getUser().getId();

        面对分布式系统失效，因为每个服务器有单独的jvm，string池就不唯一了。
       synchronized (id.toString().intern()){
            //获取spring代理的对象
           IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
            return proxy.createVochuerOrder(voucherId);
        }
        //创建分布式锁对象
       // SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate, "order" + userId);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock(1L, TimeUnit.SECONDS);
        //加锁失败
        if (!isLock) {
            return Result.fail("不允许重复下单");
        }
        try {
            //获取代理对象(事务)
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVochuerOrder(voucherId);
        }finally {
            //释放锁
            lock.unlock();
        }

    }
       */
 //异步处理线程池
 private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    //在类初始化之后执行，因为当这个类初始化好了之后，随时都是有可能要执行的
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    // 用于线程池处理的任务 当初始化完毕后，就会去从对列中去拿信息
    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有消息，继续下一次循环
                        continue;
                    }

                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3.创建订单
                    createVoucherOrder(voucherOrder);
                    // 4.确认消息 XACK
                    stringRedisTemplate.opsForStream().acknowledge("stream.orders", "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    //处理异常消息
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create("stream.orders", ReadOffset.from("0"))
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有异常消息，结束循环
                        break;
                    }
                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3.创建订单
                    createVoucherOrder(voucherOrder);
                    // 4.确认消息 XACK
                    //这里有一个坑，是我换了redis的数据库2号，但我在cmd操作的是0号仓库所以导致一直连接查找失败
                    stringRedisTemplate.opsForStream().acknowledge("stream.orders", "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pendding订单异常", e);
                    try{
                        Thread.sleep(20);
                    }catch(InterruptedException ex){
                        ex.printStackTrace();
                    }
                }
            }
        }
    }

    /*  private class VoucherOrderHandler implements  Runnable{
    //实现run方法
        @Override
        public void run() {
            while(true){
                try {
                    //获取阻塞队列中的信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    //创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                    log.error("获取订单异常",e);
                }
            }
        }
    }
    //阻塞队列
    private BlockingQueue<VoucherOrder> orderTasks =new ArrayBlockingQueue<>(1024 * 1024);
   */
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
     //这里的加锁是为了保底作用，一般不会出现并发安全了，因为前面的脚本判断已经加锁了
        //1.获取用户
        Long userId = voucherOrder.getUserId();
        // 2.创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 3.尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 4.判断是否获得锁成功
        if (!isLock) {
            // 获取锁失败，直接返回失败或者重试
            log.error("不允许重复下单！");
            return;
        }
        try {
            //注意：由于是spring的事务是放在threadLocal中，此时的是多线程，事务会失效
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }


 private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        //返回类型
        SECKILL_SCRIPT.setResultType(Long.class);
    }
 /*@Override
 public Result seckillVoucher(Long voucherId) throws InterruptedException {
     //获取用户
     Long userId = UserHolder.getUser().getId();
     long orderId = redisIdWorker.nextId("order");
        //执行lua脚本,第一个参数位脚本内容，第二个参数位key集合，第三个参数为value集合
     Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
             Collections.emptyList(),
             voucherId.toString(),userId);
     int r = result.intValue();
     //校验返回信息
     if (r != 0) {
         // 2.1.不为0 ，代表没有购买资格
         return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
     }
     //TODO 保存阻塞队列
     VoucherOrder voucherOrder = new VoucherOrder();
     voucherOrder.setId(orderId);
     // 2.4.用户id
     voucherOrder.setUserId(userId);
     // 2.5.代金券id
     voucherOrder.setVoucherId(voucherId);
     // 2.6.放入阻塞队列
     orderTasks.add(voucherOrder);
     //3.获取代理对象,子线程从主线程里拿到这个代理对象
     proxy = (IVoucherOrderService)AopContext.currentProxy();
     // 3.返回订单id
     return Result.ok(orderId);
     //返回用户id
 }
*/
 public Result seckillVoucher(Long voucherId) throws InterruptedException {
     //获取用户Id
     Long userId = UserHolder.getUser().getId();
     //获取订单Id
     long orderId = redisIdWorker.nextId("order");
     //执行lua脚本,第一个参数位脚本内容，第二个参数位key集合，第三个参数为value集合
     Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(),
             voucherId.toString(),userId.toString(),String.valueOf(orderId));
     int r = result.intValue();
     //校验返回信息
     if (r != 0) {
         // 2.1.不为0 ，代表没有购买资格
         return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
     }
     //3.获取代理对象,子线程从主线程里拿到这个代理对象,初始化代理对象
     proxy = (IVoucherOrderService)AopContext.currentProxy();
     // 3.返回订单id
     return Result.ok(orderId);
     //返回用户id
 }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //一人一单
        Long userid = voucherOrder.getId();
        Integer count = query().eq("user_id", userid).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if(count > 0){
            // 用户已经购买过了
           log.error("用户已经购买过一次！");
        }
        //5，扣减库存,扣减的是数据库的
        boolean success = seckillVoucherService.update()
                .setSql("stock= stock -1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock",0).update(); //where id = ? and stock > 0
        if (!success) {
            //扣减库存
           log.error("库存不足！");
        }
        save(voucherOrder);
    }
}

