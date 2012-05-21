package com.emc.greenplum.hadoop;

import com.emc.greenplum.hadoop.plugins.*;
import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 5/18/12
 */
public class Hdfs  {
    private HdfsFileSystem fs = null;

    public Hdfs(HdfsFileSystem fs) {
        this.fs = fs;
    }

    public List<HdfsEntity> list(String path) {
        try {
            return fs.glob("/");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
