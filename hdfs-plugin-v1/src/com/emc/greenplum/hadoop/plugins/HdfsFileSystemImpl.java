package com.emc.greenplum.hadoop.plugins;

import org.xeustechnologies.jcl.JarClassLoader;
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
    private JarClassLoader hadoopCl;

    @Override
    public void loadDependencies() {
        hadoopCl.add(getClass().getClassLoader().getResource("META-INF/external-deps/commons-logging-1.1.1.jar"));
        hadoopCl.add(getClass().getClassLoader().getResource("META-INF/external-deps/commons-lang-2.4.jar"));
        hadoopCl.add(getClass().getClassLoader().getResource("META-INF/external-deps/commons-configuration-1.6.jar"));
        hadoopCl.add(getClass().getClassLoader().getResource("META-INF/external-deps/hadoop-core-1.0.0.jar"));
    }

    @Override
    public void loadFileSystem(String host, String port, String username) {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(hadoopCl);

        Configuration config = new Configuration();
        config.set("fs.default.name", "hdfs://" + host + ":" + port);
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        try {
            fileSystem = FileSystem.get(FileSystem.getDefaultUri(config), config, username);
        } catch (Exception e) {
           // e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
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
    public void setClassLoader(JarClassLoader classLoader) {
        hadoopCl = classLoader;
    }

    @Override
    public boolean loadedSuccessfully() {
        return fileSystem != null;
    }
}
