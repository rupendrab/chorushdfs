package com.emc.greenplum.service.resources;

import com.emc.greenplum.hadoop.Hdfs;
import com.emc.greenplum.hadoop.plugins.HdfsFileSystem;

import java.util.concurrent.Callable;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 6/12/12
 * Time: 5:31 PM
 * To change this template use File | Settings | File Templates.
 */
public class HdfsTerminator implements Callable {
    private HdfsFileSystem hdfs;

    public HdfsTerminator(HdfsFileSystem hdfs) {
        this.hdfs = hdfs;
    }

    @Override
    public Object call() throws Exception {
        hdfs.closeFileSystem();
        return null;
    }
}
