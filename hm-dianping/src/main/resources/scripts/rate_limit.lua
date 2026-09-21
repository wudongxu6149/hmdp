--[[ 【阶段6新增】滑动窗口限流脚本（ZSet 实现，Lua 保证"清理→计数→记录"原子）
  KEYS[1] 限流计数器：rate_limit:{业务}:{限流类型}[:{用户ID/IP}]
  ARGV[1] 当前毫秒时间戳
  ARGV[2] 窗口长度（毫秒）
  ARGV[3] 窗口内最大允许次数
  ARGV[4] 本次请求的唯一成员（时间戳+UUID，防同毫秒覆盖）

  返回：1=放行  0=拒绝
  相比固定窗口（INCR+EXPIRE）：滑动窗口没有"窗口边界突刺"——任意连续 window 毫秒内的
  请求总数都不会超过 maxCount；Redis 单线程保证脚本原子，无需额外加锁
]]
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local maxCount = tonumber(ARGV[3])
local member = ARGV[4]

-- 1.清理窗口之外的旧记录（score 即请求时间戳）
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

-- 2.统计当前窗口内的请求次数
local count = redis.call('ZCARD', key)
if count >= maxCount then
    return 0
end

-- 3.未超限：记录本次请求，key 过期时间设为略大于窗口（冷 key 自动清理）
redis.call('ZADD', key, now, member)
redis.call('PEXPIRE', key, window + 1000)

return 1
