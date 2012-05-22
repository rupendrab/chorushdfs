package com.emc.greenplum.hadoop.plugins;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 5/21/12
 * Time: 3:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class HdfsFileSystemImpl implements HdfsFileSystem {
    FileSystem fileSystem;
    private ClassLoader hadoopCl;

    @Override
    public void loadDependencies() {
    }

    @Override
    public void loadFileSystem(String host, String port, String username) {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(hadoopCl);

        Configuration config = new Configuration();
        config.set("fs.default.name", "hdfs://" + host + ":" + port);
        config.set("hadoop.jobs.ugi", username + "," + username);
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");


        try {
            fileSystem = FileSystem.get(config);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }

    }

    @Override
    public List<HdfsEntity> glob(String path) throws IOException {
        FileStatus[] fileStatuses = fileSystem.globStatus(new Path(path));
        List<HdfsEntity> entities = new ArrayList<HdfsEntity>();

        for(FileStatus fileStatus: fileStatuses) {
            HdfsEntity entity = new HdfsEntity();

            entity.setDirectory(fileStatus.isDir());
            entity.setPath(fileStatus.getPath().toUri().getPath());
            entity.setModifiedAt(new Time(fileStatus.getModificationTime()));
            entity.setSize(fileStatus.getLen());

            entities.add(entity);
        }

        return entities;
    }

    @Override
    public void setClassLoader(ClassLoader classLoader) {
        hadoopCl = classLoader;
    }
}
