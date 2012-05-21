package com.emc.greenplum.hadoop;

import com.emc.greenplum.hadoop.HdfsEntity;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 5/18/12
 */
public interface HdfsFileSystem {

    public void loadDependencies() ;

    public void loadFileSystem(String host, String port, String username) ;

    public List<HdfsEntity> glob (String path) throws IOException;

    public void setClassLoader(ClassLoader classLoader);

}
