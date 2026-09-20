--[[ 【阶段2 全量重写 / 阶段4 简化】秒杀资格判定脚本

  原子能力：时间窗校验 → 库存校验 → 一人一单 → 扣库存 → 记资格 → 写事务标记
  相比旧版：① 补上秒杀时间窗校验（旧版未开始也能抢）；② 删除 XADD stream.orders（Stream 链路废弃）；
           ③ key 全部改由 KEYS 传入（Redis Cluster 规范写法，预留集群迁移结构）

  KEYS[1] 秒杀元数据 Hash  seckill:voucher:{voucherId}（stock/beginTime/endTime，新增秒杀券时预热）
  KEYS[2] 一人一单资格 Set seckill:order:{voucherId}
  KEYS[3] 事务标记         seckill:tx:{orderId}（供 Broker 回查）
  ARGV[1] 当前毫秒时间戳（Java 传入，用于时间窗比较）
  ARGV[2] userId
]]
local voucherKey = KEYS[1]
local orderKey = KEYS[2]
local txKey = KEYS[3]
local now = tonumber(ARGV[1])
local userId = ARGV[2]

-- 1.时间窗校验：修复旧版"秒杀未开始也能抢"的缺陷。
--   元数据未预热（beginTime 为空）按未开始处理，避免脚本对 nil 比较报错
local beginTime = tonumber(redis.call('hget', voucherKey, 'beginTime'))
local endTime = tonumber(redis.call('hget', voucherKey, 'endTime'))
if (not beginTime or now < beginTime) then
    return 3
end
if (not endTime or now > endTime) then
    return 4
end

-- 2.库存校验：不足或未预热均视为无货
local stock = tonumber(redis.call('hget', voucherKey, 'stock'))
if (not stock or stock <= 0) then
    return 1
end

-- 3.一人一单：资格 Set 内已存在该用户则拒绝（旧版逻辑保留）
if (redis.call('SISMEMBER', orderKey, userId) == 1) then
    return 2
end

-- 4.扣减库存 + 记资格（同一脚本内原子完成）
redis.call('hincrby', voucherKey, 'stock', -1)
redis.call('sadd', orderKey, userId)

-- 5.事务标记：与上面的扣减在同一 Lua 内原子写入——
--   标记存在 = Redis 已扣 = 消息必须投递（Broker 回查的唯一事实依据）；TTL 1天远大于回查窗口
redis.call('set', txKey, '1', 'EX', 86400)

-- 成功
return 0
