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
    public List<HdfsEntity> sayHello(@QueryParam("name") Optional<String> name) {

        JarClassLoader jarClassLoader = new JarClassLoader();
        jarClassLoader.add(getClass().getClassLoader().getResource("META-INF/external-deps/commons-logging-1.0.4.jar"));
        jarClassLoader.add(getClass().getClassLoader().getResource("META-INF/external-deps/hadoop-0.20.1gp-core.jar"));
        jarClassLoader.add(getClass().getClassLoader().getResource("META-INF/plugins/hdfs-plugin-v0-20-1gp-0.0.1.jar"));

        JclObjectFactory objectFactory = JclObjectFactory.getInstance();
        Object hdfsObject = objectFactory.create(jarClassLoader, "com.emc.greenplum.hadoop.plugins.HdfsFileSystemImpl");

        HdfsFileSystem fileSystem = (HdfsFileSystem) JclUtils.toCastable(hdfsObject, HdfsFileSystem.class);
        fileSystem.setClassLoader(jarClassLoader);

        fileSystem.loadFileSystem("gillette", "8020", "pivotal");

        try {
            return fileSystem.glob("/*");
        } catch (IOException e) {
            e.printStackTrace();
            return new ArrayList<HdfsEntity>();
        }
    }
}