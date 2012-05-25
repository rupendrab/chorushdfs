package com.emc.greenplum.hadoop;

import com.emc.greenplum.hadoop.plugins.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.xeustechnologies.jcl.JarClassLoader;
import org.xeustechnologies.jcl.JclObjectFactory;
import org.xeustechnologies.jcl.JclUtils;

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

        fileSystem = loadPlugin(version);
        fileSystem.loadFileSystem(host, port, username);
    }

    public HdfsVersion getServerVersion() {
        HdfsFileSystem fileSystem = null;

        for(HdfsVersion version: HdfsVersion.values()) {
            fileSystem = loadPlugin(version);
            fileSystem.loadFileSystem(host, port, username);

            if(fileSystem.loadedSuccessfully()) {
                fileSystem.closeFileSystem();
                return version;
            }
        }
        return null;
    }

    private HdfsFileSystem loadPlugin(HdfsVersion version) {
        JarClassLoader jarClassLoader = new JarClassLoader();

        HdfsFileSystem hdfsFileSystem = loadObjectFromPlugin(jarClassLoader, version);
        hdfsFileSystem.setClassLoader(jarClassLoader);

        return hdfsFileSystem;
    }

    private HdfsFileSystem loadObjectFromPlugin(JarClassLoader jarClassLoader, HdfsVersion version) {
        jarClassLoader.add(getClass().getClassLoader().getResource(version.getPluginJar()));

        for(String dependency: version.getDependencies()) {
            jarClassLoader.add(getClass().getClassLoader().getResource(dependency));
        }

        JclObjectFactory objectFactory = JclObjectFactory.getInstance();
        Object hdfsObject = objectFactory.create(jarClassLoader, "com.emc.greenplum.hadoop.plugins.HdfsFileSystemImpl");

        return (HdfsFileSystem) JclUtils.toCastable(hdfsObject, HdfsFileSystem.class);
    }

    public List<HdfsEntity> list(String path) {
        try {
            return fileSystem.list(path);
        } catch (IOException e) {
            e.printStackTrace();
            return new ArrayList<HdfsEntity>();
        }
    }

    public List<String> content(String path) throws IOException {
        return fileSystem.getContent(path);
    }

    public void closeFileSystem() {
        fileSystem.closeFileSystem();
    }
}
