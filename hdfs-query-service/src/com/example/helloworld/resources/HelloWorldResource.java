package com.example.helloworld.resources;

import com.emc.greenplum.hadoop.plugins.*;

import com.google.common.base.Optional;
import com.yammer.metrics.annotation.Timed;
import org.xeustechnologies.jcl.JarClassLoader;
import org.xeustechnologies.jcl.JclObjectFactory;
import org.xeustechnologies.jcl.JclUtils;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Path("/hello-world")
@Produces(MediaType.APPLICATION_JSON)
public class HelloWorldResource {
    private final String template;
    private final String defaultName;
    private final AtomicLong counter;

    public HelloWorldResource(String template, String defaultName) {
        this.template = template;
        this.defaultName = defaultName;
        this.counter = new AtomicLong();
    }

    @GET
    public List<HdfsEntity> sayHello(@QueryParam("host") String host) {

        String[] versions = {"META-INF/plugins/hdfs-plugin-v1-0.0.1.jar", "META-INF/plugins/hdfs-plugin-v0-20-1gp-0.0.1.jar"};
        HdfsFileSystem fileSystem = null;

        for(String version: versions) {

            System.out.println("Trying to load version " + version);

            JarClassLoader jarClassLoader = new JarClassLoader();
            jarClassLoader.add(getClass().getClassLoader().getResource(version));

            JclObjectFactory objectFactory = JclObjectFactory.getInstance();
            Object hdfsObject = objectFactory.create(jarClassLoader, "com.emc.greenplum.hadoop.plugins.HdfsFileSystemImpl");

            fileSystem = (HdfsFileSystem) JclUtils.toCastable(hdfsObject, HdfsFileSystem.class);
            fileSystem.setClassLoader(jarClassLoader);
            fileSystem.loadDependencies();

            fileSystem.loadFileSystem(host, "8020", "pivotal");

            if(fileSystem.loadedSuccessfully()) {
                System.out.println("Ok, version "+ version + " actually worked!");
                break;
            } else {
                fileSystem = null;
                System.out.println("Loading " + version + " didn't work, trying another one");
            }

        }


        try {
            return fileSystem.glob("/*");
        } catch (IOException e) {
            e.printStackTrace();
            return new ArrayList<HdfsEntity>();
        }
    }
}