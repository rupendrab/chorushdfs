package com.example.helloworld.resources;

import com.emc.greenplum.hadoop.Hdfs;
import com.emc.greenplum.hadoop.HdfsVersion;
import com.emc.greenplum.hadoop.plugins.*;

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
    public String sayHello(@QueryParam("host") String host, @QueryParam("port") String port, @QueryParam("username") String username) {
        Hdfs hdfs = new Hdfs(host, port, username);
        HdfsVersion version = hdfs.getServerVersion();

        return version.getName();
    }
}