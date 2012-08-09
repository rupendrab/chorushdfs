package com.emc.greenplum.hadoop.plugins;

import org.xeustechnologies.jcl.JarClassLoader;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 8/9/12
 * Time: 4:08 PM
 * To change this template use File | Settings | File Templates.
 */
public interface HdfsFileSystem {
    void loadFileSystem(String host, String port, String username);

    void closeFileSystem();

    List<HdfsEntity> list(String path) throws IOException;

    boolean loadedSuccessfully();

    void setClassLoader(JarClassLoader classLoader);

    List<String> getContent(String path, int lineCount) throws IOException;
}
