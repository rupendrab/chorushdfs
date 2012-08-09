package com.emc.greenplum.hadoop.plugin;

import com.emc.greenplum.hadoop.HdfsVersion;
import com.emc.greenplum.hadoop.plugins.HdfsFileSystem;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HdfsCachedPluginLoader {
    private Map<HdfsVersion, HdfsPluginLoader> pluginCache = new ConcurrentHashMap<HdfsVersion, HdfsPluginLoader>();
    private HdfsPluginBuilder builder;

    public HdfsCachedPluginLoader(HdfsPluginBuilder builder) {
        this.builder = builder;
    }

    public HdfsFileSystem loadPlugin(HdfsVersion version) {
        if(pluginCache.containsKey(version)) {
            return pluginCache.get(version).loadObjectFromPlugin();
        } else {
            return loadPluginFromScratch(version);
        }
    }

    private HdfsFileSystem loadPluginFromScratch(HdfsVersion version) {
        HdfsPluginLoader pluginLoader = builder.build(version);
        pluginCache.put(version, pluginLoader);

        return pluginLoader.loadObjectFromPlugin();
    }
}
