package com.emc.greenplum.hadoop.plugins;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 5/21/12
 * Time: 3:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class HdfsFileSystemImpl extends HdfsFileSystemPlugin {
    private FileSystem fileSystem;

    @Override
    public void loadFileSystem(String host, String port, String username) {
        loadHadoopClassLoader();

        Configuration config = new Configuration();
        config.set("fs.default.name", "hdfs://" + host + ":" + port);
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        try {
            fileSystem = FileSystem.get(FileSystem.getDefaultUri(config), config, username);
        } catch (Exception e) {
        } finally {
            restoreOriginalClassLoader();
        }
    }

    @Override
    public void closeFileSystem() {
        try {
            fileSystem.close();
        } catch (IOException e) {
        }
        fileSystem = null;
    }

    @Override
    public List<HdfsEntity> list(String path) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
        List<HdfsEntity> entities = new ArrayList<HdfsEntity>();

        for(FileStatus fileStatus: fileStatuses) {
            HdfsEntity entity = new HdfsEntity();

            entity.setDirectory(fileStatus.isDir());
            entity.setPath(fileStatus.getPath().toUri().getPath());
            entity.setModifiedAt(new Date(fileStatus.getModificationTime()));
            entity.setSize(fileStatus.getLen());

            if(fileStatus.isDir()) {
                FileStatus[] contents = fileSystem.listStatus(fileStatus.getPath());
                entity.setContentCount(contents.length);
            }

            entities.add(entity);
        }

        return entities;
    }

    @Override
    public DataInputStream open(String path) throws IOException {
        return fileSystem.open(new Path(path));
    }

    @Override
    public boolean loadedSuccessfully() {
        return fileSystem != null;
    }
}
