-- 幂等回补：库存增加、必要时释放资格，必须与退票标记一次完成。
-- KEYS[1] seckill:refund:{orderId}  KEYS[2] seckill:voucher:{voucherId}
-- KEYS[3] seckill:order:{voucherId}  ARGV[1] userId  ARGV[2] 是否释放资格

--判断先前是否退过货，如果有标记直接返回
if redis.call('exists', KEYS[1]) == 1 then
    return 0
end

-- Lua 运行时错误不会回滚此前的写入，先校验会影响后续操作的 Key。
-- 获取优惠券的库存
local stock = redis.call('hget', KEYS[2], 'stock')
-- 校验库存是否是一个正数 %在lua中为转义字符
if not stock or not string.match(stock, '^%-?%d+$') then
    return redis.error_reply('invalid seckill stock for refund')
end

-- 判断是否要释放资格 releaseQualification=true（1），false=0
if ARGV[2] == '1' then
    local orderType = redis.call('type', KEYS[3]).ok
    if orderType ~= 'none' and orderType ~= 'set' then
        return redis.error_reply('invalid seckill order key type')
    end
end
-- 添加退款标记
redis.call('set', KEYS[1], '1', 'EX', 86400)
-- 恢复库存
redis.call('hincrby', KEYS[2], 'stock', 1)

-- 恢复用户的购买资格
if ARGV[2] == '1' then
    redis.call('srem', KEYS[3], ARGV[1])
end
return 1
