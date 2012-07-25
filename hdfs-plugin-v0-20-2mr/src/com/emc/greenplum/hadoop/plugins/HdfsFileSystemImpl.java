package com.emc.greenplum.hadoop.plugins;

import org.xeustechnologies.jcl.JarClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.lang.reflect.Field;
import java.net.URL;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 5/21/12
 * Time: 3:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class HdfsFileSystemImpl extends HdfsFileSystemPlugin {
    private FileSystem fileSystem;

    @Override
    public void loadFileSystem(String host, String port, String username) {
        prepareDynamicLibraries();

        loadHadoopClassLoader();

        Configuration config = new Configuration();
        config.set("fs.default.name", "hdfs://" + host + ":" + port);
        config.set("hadoop.job.ugi", username + ", " + username);
        config.set("fs.hdfs.impl", "com.mapr.fs.MapRFileSystem");

        try {
            fileSystem = FileSystem.get(config);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            restoreOriginalClassLoader();
        }
    }

    private void prepareDynamicLibraries() {
        String osPath = System.getProperty("os.name").replace(" ", "_") + "-" + System.getProperty("os.arch");

        String jarPathMac = "META-INF/external-deps/native/" + osPath + "/libMapRClient.dylib";
        String jarPathLinux = "META-INF/external-deps/native/" + osPath + "/libMapRClient.so";
        String outputPath = System.getProperty("java.io.tmpdir");

        try {
            copyFileFromJar(jarPathMac, "dylib", outputPath);
            copyFileFromJar(jarPathLinux, "so", outputPath);

            addDir(outputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void copyFileFromJar(String jarPath, String extension, String outputPath) {
        try {
            InputStream in = hadoopCl.getResourceAsStream(jarPath);
            if (in == null) { return; }

            File libFile = new File(outputPath, "libMapRClient." + extension);
            libFile.deleteOnExit();

            OutputStream out = new BufferedOutputStream(new FileOutputStream(libFile));

            int len = 0;
            byte[] buffer = new byte[8192];
            while ((len = in.read(buffer)) > -1)
                out.write(buffer, 0, len);
            out.close();
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addDir(String s) throws IOException {
        try {
            Field field = ClassLoader.class.getDeclaredField("usr_paths");
            field.setAccessible(true);
            String[] paths = (String[]) field.get(null);
            for (int i = 0; i < paths.length; i++) {
                if (s.equals(paths[i])) {
                    return;
                }
            }
            String[] tmp = new String[paths.length + 1];
            System.arraycopy(paths, 0, tmp, 0, paths.length);

            tmp[paths.length] = s;
            field.set(null, tmp);
            System.setProperty("java.library.path", System.getProperty("java.library.path") + File.pathSeparator + s);
        } catch (IllegalAccessException e) {
            throw new IOException("Failed to get permissions to set library path");
        } catch (NoSuchFieldException e) {
            throw new IOException("Failed to get field handle to set library path");
        }
    }


    @Override
    public void closeFileSystem() {
        try {
            fileSystem.closeAll();
        } catch (IOException e) {
        }
        fileSystem = null;
    }

    @Override
    public List<HdfsEntity> list(String path) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
        List<HdfsEntity> entities = new ArrayList<HdfsEntity>();

        for (FileStatus fileStatus : fileStatuses) {
            HdfsEntity entity = new HdfsEntity();

            entity.setDirectory(fileStatus.isDir());
            entity.setPath(fileStatus.getPath().toUri().getPath());
            entity.setModifiedAt(new Time(fileStatus.getModificationTime()));
            entity.setSize(fileStatus.getLen());

            if (fileStatus.isDir()) {
                try {
                    FileStatus[] contents = fileSystem.listStatus(fileStatus.getPath());
                    entity.setContentCount(contents.length);
                } catch (Exception exception) {
                    entity.setContentCount(-1);
                }
            }

            entities.add(entity);
        }

        return entities;
    }

    @Override
    public List<String> getContent(String path, int lineCount) throws IOException {
        DataInputStream in = (DataInputStream) fileSystem.open(new Path(path));

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


    @Override
    public boolean loadedSuccessfully() {
        return fileSystem != null;
    }
}
