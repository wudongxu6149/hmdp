package com.hmdp.utils.lock;

public interface ILock {

    public boolean tryLock(long timeoutSec);

    public void unLock();
}
