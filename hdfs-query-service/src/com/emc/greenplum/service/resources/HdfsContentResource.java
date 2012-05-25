package com.emc.greenplum.service.resources;

import com.emc.greenplum.hadoop.Hdfs;
import com.emc.greenplum.hadoop.HdfsVersion;
import com.emc.greenplum.service.SimpleResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 5/22/12
 * Time: 12:07 PM
 * To change this template use File | Settings | File Templates.
 */

@Path("/{version}/show/{path}")
@Produces(MediaType.APPLICATION_JSON)
public class HdfsContentResource {

    @GET
    public List<String> getContent(@PathParam("version") String versionName,
                                        @PathParam("path") String path,
                                        @QueryParam("host") String host,
                                        @QueryParam("port") String port,
                                        @QueryParam("username") String username) {

        System.out.println("Showing contents of file " + path);
        HdfsVersion version = HdfsVersion.findVersion(versionName);
        Hdfs hdfs = new Hdfs(host, port, username, version);

        List<String> content = new ArrayList<String>();

        try {
            content = hdfs.content(path);
        } catch (IOException e) {
        } finally {
            hdfs.closeFileSystem();
            return content;
        }
    }
}
