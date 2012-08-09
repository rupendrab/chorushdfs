package com.emc.greenplum.hadoop.commands;

import com.emc.greenplum.hadoop.plugins.HdfsFileSystem;

import java.util.concurrent.Callable;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 6/12/12
 * Time: 5:31 PM
 * To change this template use File | Settings | File Templates.
 */
public class HdfsCloseFileSystemCommand implements Callable {
    private HdfsFileSystem fileSystem;

    public HdfsCloseFileSystemCommand(HdfsFileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public Object call() throws Exception {
        fileSystem.closeFileSystem();
        return null;
    }
}
