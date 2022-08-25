package com.aiger.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class HDFSClient {

    public static void main(String[] args) throws IOException {

        //创建服务器端代理，即客户端
        RPCProtocol client = RPC.getProxy(   // 通过RPC创建服务器端代理对象，即客户端
                RPCProtocol.class,    // 指定通信协议
                RPCProtocol.versionID,    // 指定通信协议的版本号
                new InetSocketAddress("localhost", 8888),   // 指明服务器端地址和端口号
                new Configuration()   // 指定配置信息
        );

        System.out.println("我是客户端");

        // 利用客户端对象调用madirs方法
        client.mkdirs("/goblin");
    }
}
