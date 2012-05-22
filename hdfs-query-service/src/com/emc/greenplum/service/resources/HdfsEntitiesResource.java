package com.emc.greenplum.service.resources;

import com.emc.greenplum.hadoop.Hdfs;
import com.emc.greenplum.hadoop.HdfsVersion;
import com.emc.greenplum.hadoop.plugins.HdfsEntity;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 5/22/12
 * Time: 11:12 AM
 * To change this template use File | Settings | File Templates.
 */
@Path("/{version}/list/{path}")
@Produces(MediaType.APPLICATION_JSON)
public class HdfsEntitiesResource {

    @GET
    public List<HdfsEntity> getEntities(@PathParam("version") String versionName,
                            @PathParam("path") String path,
                            @QueryParam("host") String host,
                            @QueryParam("port") String port,
                            @QueryParam("username") String username) {

        HdfsVersion version = HdfsVersion.findVersion(versionName);
        Hdfs hdfs = new Hdfs(host, port, username, version);

        List<HdfsEntity> entities = hdfs.glob(path + "*");
        hdfs.closeFileSystem();

        return entities;
    }
}
