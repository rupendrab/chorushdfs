package com.emc.greenplum.hadoop.plugin;

import com.emc.greenplum.hadoop.HdfsVersion;
import com.emc.greenplum.hadoop.plugins.HdfsFileSystem;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 8/8/12
 * Time: 7:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class HdfsPluginBuilder {
    public HdfsPluginLoader build(HdfsVersion version) {
        return new HdfsPluginLoader(version);
    }

    public HdfsFileSystem fileSystem(HdfsVersion version) {
        return build(version).loadObjectFromPlugin();
    }
}
