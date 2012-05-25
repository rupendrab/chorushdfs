package com.emc.greenplum.hadoop.plugins;

import org.xeustechnologies.jcl.JarClassLoader;
import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 5/18/12
 */
public interface HdfsFileSystem {

    public void loadFileSystem(String host, String port, String username) ;

    public void closeFileSystem();

    public List<HdfsEntity> list(String path) throws IOException;

    public void setClassLoader(JarClassLoader classLoader);

    public boolean loadedSuccessfully();

    List<String> getContent(String path) throws IOException;
}
