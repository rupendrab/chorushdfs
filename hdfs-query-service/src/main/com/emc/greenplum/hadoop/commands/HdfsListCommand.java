package com.emc.greenplum.hadoop.commands;

import com.emc.greenplum.hadoop.plugins.HdfsEntity;
import com.emc.greenplum.hadoop.plugins.HdfsFileSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by IntelliJ IDEA.
 * User: pivotal
 * Date: 7/20/12
 * Time: 4:39 PM
 * To change this template use File | Settings | File Templates.
 */
public class HdfsListCommand implements Callable<List<HdfsEntity>> {
    private HdfsFileSystem fileSystem;
    private String path;

    public HdfsListCommand(HdfsFileSystem fileSystem, String path) {
        this.fileSystem = fileSystem;
        this.path = path;
    }

    public List<HdfsEntity> call() {
        try {
            return fileSystem.list(path);
        } catch (IOException e) {
            return new ArrayList<HdfsEntity>();
        }
    }
}
