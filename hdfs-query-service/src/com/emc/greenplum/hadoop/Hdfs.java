package com.emc.greenplum.hadoop;

import com.emc.greenplum.hadoop.commands.HdfsCloseFileSystemCommand;
import com.emc.greenplum.hadoop.commands.HdfsContentCommand;
import com.emc.greenplum.hadoop.commands.HdfsFileSystemLoaderCommand;
import com.emc.greenplum.hadoop.commands.HdfsListCommand;
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

    public static int timeout = 5;

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

            int time = (int)Math.ceil((double)timeout / (double)HdfsVersion.values().length);
            protectTimeout(time, new HdfsFileSystemLoaderCommand(fileSystem, host, port, username));

            if(fileSystem.loadedSuccessfully()) {
                fileSystem.closeFileSystem();
                return version;
            }
        }
        return null;
    }

    public List<HdfsEntity> list(String path) {
        try {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<List<HdfsEntity>> future = executor.submit(new HdfsListCommand(fileSystem, path));

            return future.get(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            return null;
        }
    }

    public List<String> content(String path) throws IOException {
        try {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<List<String>> future = executor.submit(new HdfsContentCommand(fileSystem, path));

            return future.get(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            return null;
        }
    }

    public void closeFileSystem() {
        protectTimeout(timeout, new HdfsCloseFileSystemCommand(fileSystem));
    }

    private void protectTimeout(int seconds, Callable command) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<String> future = executor.submit(command);

        try {
            future.get(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
        }
    }
}
