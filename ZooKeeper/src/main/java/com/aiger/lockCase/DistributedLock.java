package com.aiger.lockCase;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DistributedLock {
    private final String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private final int sessionTimeout = 2000;
    private ZooKeeper zk;
    private String currentNode;
    private String waitPath;

    private CountDownLatch waitLatch = new CountDownLatch(1);

    //重写构造方法，将zk的连接放到构造方法中；
    public DistributedLock() throws IOException, KeeperException, InterruptedException {
        //创建zk连接对象；
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType() == Event.EventType.NodeDeleted && watchedEvent.getPath().equals(waitPath)){
                    waitLatch.countDown();
                }
            }
        });
        // 判断/locks路径是否存在；
        Stat stat = zk.exists("/locks", false);
        if (stat == null) {
            zk.create("/locks", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    // 加锁方法
    public void zkLock() throws KeeperException, InterruptedException {

        //创建临时节点
        currentNode = zk.create("/locks/seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        Thread.sleep(10);
        //获取/locks的子节点
        List<String> children = zk.getChildren("/locks", false);
        if (children.size() == 1) {
            return;
        } else {
            //排序
            Collections.sort(children);
            //获取当前节点的名称
            String currentNodeName = currentNode.substring("/locks/".length());
            //获取当前节点的索引
            int index = children.indexOf(currentNodeName);
            if (index == -1) {
                System.out.println("数据错误");
            } else if (index == 0) {
                return;
            } else {
                //获取上一个节点的路径
                waitPath = "/locks/" + children.get(index - 1);
                //监控上一个节点的增删变化；
                zk.getData(waitPath , true , new Stat());
                //进入等待锁机制
                waitLatch.await();
                return;
            }
        }
    }

    public void zkUnLock() throws KeeperException, InterruptedException {

        zk.delete(currentNode , -1);
    }
}
