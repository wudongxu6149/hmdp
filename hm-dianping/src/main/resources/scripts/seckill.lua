--优惠券id
local voucherId = ARGV[1]

--用户id
local userId = ARGV[2]

--订单id
local orderId=ARGV[3]


--1.数据的key
--1.1 库存key
local stockKey = 'seckill:stock:' .. voucherId
--1.2用户key
local orderKey = 'seckill:order:' .. voucherId

--2.业务
--2.1 判断库存是否充足
if (tonumber(redis.call('get', stockKey)) <= 0) then
    return 1
end
--2.2 判断用户是否下单
if (redis.call('SISMEMBER', orderKey, userId) == 1) then
    --存在，说明重复下单
    return 2
end
--2.3 扣减库存
redis.call('incrby', stockKey, -1)
--3.4 下单
redis.call('sadd', orderKey, userId)
--3.5当判断有资格进行下单后，发送消息到stream队列中
redis.call('xadd','stream.orders','*'
            ,'voucherId',voucherId
            ,'userId',userId
            ,'id',orderId)

--成功！
return 0