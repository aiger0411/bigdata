package com.aiger.curatorCase;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CuratorLockTest {

    public static void main(String[] args) {

        //创建分布式锁1
        final InterProcessMutex lock1 = new InterProcessMutex(getCuratorFramework(), "/locks");
        //创建分布式锁2
        final InterProcessMutex lock2 = new InterProcessMutex(getCuratorFramework(), "/locks");

        new Thread(new Runnable() {
            public void run() {
                try {
                    //获取锁
                    lock1.acquire();
                    System.out.println("线程1 获取到锁");
                    lock1.acquire();
                    System.out.println("线程1 再次获取到锁");

                    Thread.sleep(5* 1000);
                    //释放锁
                    lock1.release();
                    System.out.println("线程1释放锁");
                    lock1.release();
                    System.out.println("线程1再次释放锁");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            public void run() {
                try {
                    //获取锁
                    lock2.acquire();
                    System.out.println("线程2 获取到锁");
                    lock2.acquire();
                    System.out.println("线程2 再次获取到锁");

                    Thread.sleep(5* 1000);
                    //释放锁
                    lock2.release();
                    System.out.println("线程2释放锁");
                    lock2.release();
                    System.out.println("线程2再次释放锁");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    //分布式锁初始化
    private static CuratorFramework getCuratorFramework() {
        //失败重复策略
        ExponentialBackoffRetry policy = new ExponentialBackoffRetry(3000, 3);
        //创建CuratorFramework客户端
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("hadoop102:2181,hadoop103:2181,hadoop104:2181")
                .connectionTimeoutMs(2000)
                .sessionTimeoutMs(2000)
                .retryPolicy(policy)
                .build();

        //启动客户端
        client.start();

        return client;
    }
}
