package com.emc.greenplum.hadoop;

import com.emc.greenplum.hadoop.plugins.*;
import java.io.IOException;
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

    public Hdfs(String host, String port, String username) {
        this.host = host;
        this.port = port;
        this.username = username;
    }

    public HdfsVersion getServerVersion() {
        HdfsFileSystem fileSystem = null;

        for(HdfsVersion version: HdfsVersion.values()) {

            System.out.println("Trying to load version " + version.getName());

            JarClassLoader jarClassLoader = new JarClassLoader();
            jarClassLoader.add(getClass().getClassLoader().getResource(version.getPluginJar()));

            JclObjectFactory objectFactory = JclObjectFactory.getInstance();
            Object hdfsObject = objectFactory.create(jarClassLoader, "com.emc.greenplum.hadoop.plugins.HdfsFileSystemImpl");

            fileSystem = (HdfsFileSystem) JclUtils.toCastable(hdfsObject, HdfsFileSystem.class);
            fileSystem.setClassLoader(jarClassLoader);
            fileSystem.loadDependencies();

            fileSystem.loadFileSystem(host, port, username);

            if(fileSystem.loadedSuccessfully()) {
                return version;
            }

        }

        return null;
    }

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
