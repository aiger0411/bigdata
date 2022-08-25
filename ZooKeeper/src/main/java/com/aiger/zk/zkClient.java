package com.aiger.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class zkClient {
    // 注意逗号前后不能有空格
    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    @Before
    public void init() throws IOException {
        //创建ZooKeeper的客户端
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("===============================");
                //打印监听信息
                System.out.println(watchedEvent.getType() + "--" + watchedEvent.getPath());
                try {
                    //重新开启监听
                    List<String> children = zkClient.getChildren("/", true);
                    for (String child : children) {
                        System.out.println(child);
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Test
    public void create() throws KeeperException, InterruptedException {

        /*
         * 参数1：要创建的节点的路径；
         * 参数2：节点数据；
         * 参数3：节点权限；
         * 参数4：节点类型；
         * */
        String nodeCreated = zkClient.create
                ("/goblin", "Aiger loves Goblin.".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void getChildren() throws KeeperException, InterruptedException {

        //获取子节点，并开启监听
        List<String> children = zkClient.getChildren("/", true);

        //遍历子节点
        for (String child : children) {
            System.out.println(child);
        }

        //延时阻塞，使让方法等待，以便接收监视信息
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void exist() throws KeeperException, InterruptedException {

        //查看节点状态/判断节点是否存在
        Stat stat = zkClient.exists("/aiger", false);

        System.out.println(stat == null ? "no exist" : "exist");
    }

}
