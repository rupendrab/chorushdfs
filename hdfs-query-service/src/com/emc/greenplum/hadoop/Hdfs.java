package com.emc.greenplum.hadoop;

import com.emc.greenplum.hadoop.plugins.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 5/18/12
 */
public class Hdfs  {

    private String host;
    private String port;
    private String username;
    private HdfsFileSystem fileSystem;

    public Hdfs(String host, String port, String username) {
        this.host = host;
        this.port = port;
        this.username = username;
    }

    public Hdfs(String host, String port, String username, HdfsVersion version) {
        this(host, port, username);

        HdfsPluginLoader pluginLoader = new HdfsPluginLoader(version);
        fileSystem = pluginLoader.loadPlugin();
        fileSystem.loadFileSystem(host, port, username);
    }

    public Hdfs(String host, String port, String username, String versionName) {
        this(host, port,username, HdfsVersion.findVersion(versionName));
    }

    public HdfsVersion getServerVersion() {
        HdfsFileSystem fileSystem = null;

        for(HdfsVersion version: HdfsVersion.values()) {
            HdfsPluginLoader pluginLoader = new HdfsPluginLoader(version);

            fileSystem = pluginLoader.loadPlugin();
            fileSystem.loadFileSystem(host, port, username);

            if(fileSystem.loadedSuccessfully()) {
                fileSystem.closeFileSystem();
                return version;
            }
        }
        return null;
    }

    public List<HdfsEntity> list(String path) {
        try {
            return fileSystem.list(path);
        } catch (IOException e) {
            e.printStackTrace();
            return new ArrayList<HdfsEntity>();
        } catch (Exception e) {
            return null;
        }
    }

    public List<String> content(String path) throws IOException {
        try {
            return fileSystem.getContent(path);
        } catch (Exception e) {
            return null;
        }
    }

    public void closeFileSystem() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<String> future = executor.submit(new HdfsTerminator(fileSystem));
        try {
            future.get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
        }
    }
}
