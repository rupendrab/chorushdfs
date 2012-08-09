package com.emc.greenplum.hadoop.commands;

import com.emc.greenplum.hadoop.plugins.HdfsFileSystem;

import java.util.concurrent.Callable;

/**
 * Created by IntelliJ IDEA.
 * User: pivotal
 * Date: 7/20/12
 * Time: 4:25 PM
 * To change this template use File | Settings | File Templates.
 */
public class HdfsFileSystemLoaderCommand implements Callable {
    private String username;
    private String port;
    private HdfsFileSystem fileSystem;
    private String host;

    public HdfsFileSystemLoaderCommand(HdfsFileSystem fileSystem, String host, String port, String username) {
        this.fileSystem = fileSystem;
        this.host = host;
        this.port = port;
        this.username = username;
    }

    public Object call() {
        fileSystem.loadFileSystem(host, port, username);
        return  null;
    }
}
