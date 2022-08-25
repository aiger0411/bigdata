package com.aiger.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class NNSerrver implements RPCProtocol {

    @Override
    public void mkdirs(String path) {
        System.out.println("服务器端接收到了请求" + path);
    }

    public static void main(String[] args) throws IOException {

        //创建RPC服务器端对象；
        RPC.Server server = new RPC.Builder(new Configuration())
                .setBindAddress("localhost")  // 指定服务器地址；
                .setPort(8888)     //指定端口号
                .setProtocol(RPCProtocol.class)   // 指定通信协议；
                .setInstance(new NNSerrver())   // 指定服务器对象
                .build();   // 构建

        System.out.println("服务器端开始工作了");

        server.start();    // 让服务器开始工作
    }
}
