--[[ 获取锁的key ]]
local key = KEYS[1]

--[[ 获取当前线程标识 ]]
local threadId = ARGV[1]

--[[ 获取锁中的线程标识 ]]
local currentThreadId = redis.call('GET', key)

--[[ 比较线程标识是否一致，一致则释放锁 ]]
if currentThreadId == threadId then
    return redis.call('DEL', key)
end
return 0