package com.emc.greenplum.service.resources;

import com.emc.greenplum.hadoop.Hdfs;
import com.emc.greenplum.hadoop.HdfsVersion;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/version")
@Produces(MediaType.APPLICATION_JSON)
public class HdfsVersionResource {

    @GET
    public String getServerVersion(@QueryParam("host") String host, @QueryParam("port") String port, @QueryParam("username") String username) {
        Hdfs hdfs = new Hdfs(host, port, username);
        HdfsVersion version = hdfs.getServerVersion();

        return version.getName();
    }
}