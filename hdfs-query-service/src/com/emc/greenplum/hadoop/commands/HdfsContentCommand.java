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
 * Time: 4:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class HdfsContentCommand implements Callable<List<String>> {
    private HdfsFileSystem fileSystem;
    private String path;
    private int lines;

    public HdfsContentCommand(HdfsFileSystem fileSystem, String path, int lines) {
        this.fileSystem = fileSystem;
        this.path = path;
        this.lines = lines;
    }

    public List<String> call() {
        try {
            return fileSystem.getContent(path, lines);
        } catch (IOException e) {
            return new ArrayList<String>();
        }
    }
}
