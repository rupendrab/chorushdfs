package com.emc.greenplum.hadoop;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 5/22/12
 * Time: 10:35 AM
 * To change this template use File | Settings | File Templates.
 */

public enum HdfsVersion {
    V0202MAPR("0.20.2mr", "META-INF/plugins/hdfs-plugin-v0-20-2mr-0.0.1.jar", new String []{
            "META-INF/external-deps/commons-logging-1.1.1.jar",
            "META-INF/external-deps/maprfs-0.1.jar",
            "META-INF/external-deps/zookeeper-3.3.2.jar",
            "META-INF/external-deps/hadoop-0.20.2mr-core.jar"
    }),
    V1("1.0.0", "META-INF/plugins/hdfs-plugin-v1-0.0.1.jar", new String[] {
            "META-INF/external-deps/commons-logging-1.1.1.jar",
            "META-INF/external-deps/commons-lang-2.4.jar",
            "META-INF/external-deps/commons-configuration-1.6.jar",
            "META-INF/external-deps/hadoop-core-1.0.0.jar"
    }),
    V0201GP("0.20.1gp", "META-INF/plugins/hdfs-plugin-v0-20-1gp-0.0.1.jar", new String[] {
            "META-INF/external-deps/commons-logging-1.1.1.jar",
            "META-INF/external-deps/hadoop-0.20.1gp-core.jar"
    });

    private String pluginJar;
    private String name;
    private String [] dependencies;


    HdfsVersion(String name, String pluginJar, String[] dependencies) {
        this.pluginJar = pluginJar;
        this.name = name;
        this.dependencies = dependencies;
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

    public String [] getDependencies() {
        return dependencies;
    }
}
