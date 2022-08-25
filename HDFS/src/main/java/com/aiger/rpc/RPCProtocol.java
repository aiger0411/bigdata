package com.aiger.rpc;

public interface RPCProtocol {

    //版本ID
    long versionID = 666;

    //创建文件夹的方法；
    void mkdirs(String path);

}
