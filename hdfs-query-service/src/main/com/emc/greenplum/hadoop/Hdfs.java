package com.emc.greenplum.hadoop;

import com.emc.greenplum.hadoop.commands.HdfsCloseFileSystemCommand;
import com.emc.greenplum.hadoop.commands.HdfsContentCommand;
import com.emc.greenplum.hadoop.commands.HdfsFileSystemLoaderCommand;
import com.emc.greenplum.hadoop.commands.HdfsListCommand;
import com.emc.greenplum.hadoop.plugin.HdfsCachedPluginBuilder;
import com.emc.greenplum.hadoop.plugin.HdfsPluginBuilder;
import com.emc.greenplum.hadoop.plugins.*;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.*;

public class Hdfs {
    public static int timeout = 5;

    private static HdfsCachedPluginBuilder pluginLoader;
    private static PrintStream loggerStream = System.out;

    private String host;
    private String port;
    private String username;
    private HdfsFileSystem fileSystem;

    public static HdfsVersion getServerVersion(String host, String port, String username) {

        HdfsFileSystem fileSystem = null;

        for (HdfsVersion version : HdfsVersion.values()) {
            fileSystem = getPluginLoader().fileSystem(version);

            int time = (int) Math.ceil((double) timeout / (double) HdfsVersion.values().length);
            protectTimeout(time, new HdfsFileSystemLoaderCommand(fileSystem, host, port, username));

            if (fileSystem.loadedSuccessfully()) {
                fileSystem.closeFileSystem();
                return version;
            }
        }
        return null;
    }

    public static void setLoggerStream(PrintStream stream) {
        loggerStream = stream;
    }

    public Hdfs(String host, String port, String username, HdfsVersion version) {
        this.host = host;
        this.port = port;
        this.username = username;

        fileSystem = getPluginLoader().fileSystem(version);
        fileSystem.loadFileSystem(host, port, username);
    }

    public Hdfs(String host, String port, String username, String versionName) {
        this(host, port, username, HdfsVersion.findVersion(versionName));
    }

    public List<HdfsEntity> list(String path) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<List<HdfsEntity>> future = executor.submit(new HdfsListCommand(fileSystem, path));

        try {
            return future.get(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace(loggerStream);
            return null;
        } finally {
            future.cancel(true);
            executor.shutdownNow();
        }
    }

    public List<String> content(String path, int lineCount) throws IOException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<List<String>> future = executor.submit(new HdfsContentCommand(fileSystem, path, lineCount));

        try {
            return future.get(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace(loggerStream);
            return null;
        } finally {
            future.cancel(true);
            executor.shutdownNow();
        }
    }

    public void closeFileSystem() {
        protectTimeout(timeout, new HdfsCloseFileSystemCommand(fileSystem));
    }

    private static synchronized void protectTimeout(int seconds, Callable command) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<String> future = executor.submit(command);

        try {
            future.get(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace(loggerStream);
        } finally {
            future.cancel(true);
            executor.shutdownNow();
        }
    }

    private static HdfsCachedPluginBuilder getPluginLoader() {
        if (pluginLoader == null) {
            pluginLoader = new HdfsCachedPluginBuilder(new HdfsPluginBuilder());
        }
        return pluginLoader;
    }
}
