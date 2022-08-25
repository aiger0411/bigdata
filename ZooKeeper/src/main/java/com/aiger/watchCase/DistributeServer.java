package com.aiger.watchCase;

import org.apache.zookeeper.*;

import java.io.IOException;

public class DistributeServer {

    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zk;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        DistributeServer server = new DistributeServer();

        // 1. 获取连接
        server.getConnect();

        // 2. 注册(即创建节点)
        server.registServer(args[0]);

        // 3. 业务逻辑(睡眠)
        server.business();
    }

    private void business() throws InterruptedException {

        //业务逻辑
        Thread.sleep(Long.MAX_VALUE);
    }

    private void registServer(String hostname) throws KeeperException, InterruptedException {

        // 注册服务器
        String ZNode = zk.create("/servers/" + hostname, hostname.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println(hostname + " is online" + ZNode);
    }

    private void getConnect() throws IOException {

        //创建到zk的客户端连接
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }
}
