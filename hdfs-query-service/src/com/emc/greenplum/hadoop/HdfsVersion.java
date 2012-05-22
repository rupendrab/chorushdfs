package com.emc.greenplum.hadoop;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 5/22/12
 * Time: 10:35 AM
 * To change this template use File | Settings | File Templates.
 */
public enum HdfsVersion {
    V1("1.0.0", "META-INF/plugins/hdfs-plugin-v1-0.0.1.jar"),
    V0201GP("0.20.1gp", "META-INF/plugins/hdfs-plugin-v0-20-1gp-0.0.1.jar");

    private String pluginJar;
    private String name;


    HdfsVersion(String name, String pluginJar) {
        this.pluginJar = pluginJar;
        this.name = name;
    }

    public static HdfsVersion findVersion(String versionName) {
        for(HdfsVersion hdfsVersion: HdfsVersion.values()) {
            if(hdfsVersion.getName().equals(versionName)) {
                return hdfsVersion;
            }
        }

        return null;
    }

    public String getPluginJar() {
        return pluginJar;
    }

    public String getName() {
        return name;
    }
}
