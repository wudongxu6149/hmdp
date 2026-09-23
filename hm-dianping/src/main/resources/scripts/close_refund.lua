-- 关单后的 Redis 回补：同一 orderId 只加一次库存，资格和轮询结果一同更新。
-- KEYS[1] 幂等标记  KEYS[2] 秒杀券 Hash  KEYS[3] 购买资格 Set  KEYS[4] 结果标记
-- ARGV[1] userId  ARGV[2] 结果标记 TTL（秒）

-- 判断是否已有退款标记
if redis.call('exists', KEYS[1]) == 1 then
    redis.call('set', KEYS[4], 'CLOSED:超时未支付', 'EX', ARGV[2])
    return 0
end

-- Redis Lua 出错不会回滚此前写入，先校验后执行。
local stock = redis.call('hget', KEYS[2], 'stock')
-- 校验是否是整数
if not stock or not string.match(stock, '^%-?%d+$') then
    return redis.error_reply('invalid seckill stock for close refund')
end

local orderType = redis.call('type', KEYS[3]).ok

if orderType ~= 'none' and orderType ~= 'set' then
    return redis.error_reply('invalid seckill order key type')
end

-- 标记不设 TTL：数据库待回补状态可能长期保留，超时失效会导致旧任务再次加库存。
redis.call('set', KEYS[1], '1')
redis.call('hincrby', KEYS[2], 'stock', 1)
redis.call('srem', KEYS[3], ARGV[1])
redis.call('set', KEYS[4], 'CLOSED:超时未支付', 'EX', ARGV[2])
return 1
