package com.emc.greenplum.hadoop.plugins;

import org.xeustechnologies.jcl.JarClassLoader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 5/22/12
 * Time: 12:21 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class HdfsFileSystemPlugin  implements  HdfsFileSystem {
    protected JarClassLoader hadoopCl;
    protected ClassLoader originalClassLoader;

    @Override
    public void setClassLoader(JarClassLoader classLoader) {
        hadoopCl = classLoader;
    }

    protected void restoreOriginalClassLoader() {
        Thread.currentThread().setContextClassLoader(originalClassLoader);
    }

    protected void loadHadoopClassLoader() {
        originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(hadoopCl);
    }

}
