package com.example.helloworld.resources;

import com.emc.greenplum.hadoop.HdfsEntity;
import com.emc.greenplum.hadoop.HdfsFileSystem;
import com.emc.greenplum.hadoop.HdfsFileSystemImpl;
import com.example.helloworld.core.Saying;
import com.google.common.base.Optional;
import com.yammer.metrics.annotation.Timed;

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

        HdfsFileSystem fileSystem = new HdfsFileSystemImpl();
        fileSystem.loadFileSystem("gillette", "8020", "pivotal");

        try {
            return fileSystem.glob("/*");
        } catch (IOException e) {
            e.printStackTrace();
            return new ArrayList<HdfsEntity>();
        }
    }
}