package com.aiger.lockCase;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class DistributedLockTest {

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {

        //创建同步锁1
        final DistributedLock lock1 = new DistributedLock();
        //创建同步锁2
        final DistributedLock lock2 = new DistributedLock();

        //创建线程1
        new Thread(new Runnable() {
            public void run() {
                try {
                    //获取同步锁
                    lock1.zkLock();
                    System.out.println("线程1获取锁");
                    Thread.sleep(5 * 1000);
                    //释放锁
                    lock1.zkUnLock();
                    System.out.println("线程1释放锁");
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        //创建线程2
        new Thread(new Runnable() {
            public void run() {
                try {
                    //获取同步锁
                    lock2.zkLock();
                    System.out.println("线程2获取锁");
                    Thread.sleep(5 * 1000);
                    //释放锁
                    lock2.zkUnLock();
                    System.out.println("线程2释放锁");
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
