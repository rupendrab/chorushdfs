package com.emc.greenplum.hadoop.plugins;

import org.xeustechnologies.jcl.JarClassLoader;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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

    @Override
    public List<String> getContent(String path, int lineCount) throws IOException {
        DataInputStream in = open(path);

        BufferedReader dataReader = new BufferedReader(new InputStreamReader(in));
        ArrayList<String> lines = new ArrayList<String>();

        String line = dataReader.readLine();
        while (line != null && lines.size() < lineCount) {
            lines.add(line);
            line = dataReader.readLine();
        }

        dataReader.close();
        in.close();

        return lines;
    }

    protected abstract DataInputStream open(String path) throws IOException;

    protected void restoreOriginalClassLoader() {
        Thread.currentThread().setContextClassLoader(originalClassLoader);
    }

    protected void loadHadoopClassLoader() {
        originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(hadoopCl);
    }

}
