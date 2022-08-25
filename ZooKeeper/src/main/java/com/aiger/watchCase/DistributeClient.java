package com.aiger.watchCase;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributeClient {

    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zk;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        DistributeClient client = new DistributeClient();
        //1. 获取zk连接
        client.getConnect();

        //2. 获取子节点信息列表，并开启对父节点的监控
        client.getServerList();

        //3. 业务逻辑
        client.business();
    }

    private void business() throws InterruptedException {

        Thread.sleep(Long.MAX_VALUE);
    }

    private void getServerList() throws KeeperException, InterruptedException {

        //获取子节点的信息，并开启监控
        List<String> children = zk.getChildren("/servers", true);


        //创建一个集合，以便存储每一个子节点的信息，并打印
        ArrayList<String> list = new ArrayList<String>();

        //遍历所有节点，获取节点中的主机名称信息；
        for (String child : children) {

            byte[] data = zk.getData("/servers/" + child, false, null);
            list.add(new String(data));
        }

        System.out.println(list);
    }

    private void getConnect() throws IOException {

        //连接到zk集群
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

                //打印监控信息；
                System.out.println(watchedEvent.getType() + watchedEvent.getPath());

                //重新开启监控
                try {
                    getServerList();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
