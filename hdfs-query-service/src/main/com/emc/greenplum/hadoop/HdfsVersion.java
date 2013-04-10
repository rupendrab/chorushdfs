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
            "META-INF/external-deps/hadoop-core-1.0.3-gphd-1.2.0.0.jar"
    }),
    V0201GP("0.20.1gp", "META-INF/plugins/hdfs-plugin-v0-20-1gp-0.0.1.jar", new String[] {
            "META-INF/external-deps/commons-logging-1.1.1.jar",
            "META-INF/external-deps/hadoop-0.20.1gp-core.jar"
    }),
    VCDH4("2.0.0-cdh4.2.0", "META-INF/plugins/hdfs-plugin-vcdh4-0.0.1.jar", new String[] {
            "META-INF/external-deps/avro-1.7.3.jar",
            "META-INF/external-deps/commons-beanutils-core-1.8.0.jar",
            "META-INF/external-deps/commons-cli-1.2.jar",
            "META-INF/external-deps/commons-codec-1.4.jar",
            "META-INF/external-deps/commons-collections-3.2.1.jar",
            "META-INF/external-deps/commons-configuration-1.6.jar",
            "META-INF/external-deps/commons-daemon-1.0.3.jar",
            "META-INF/external-deps/commons-digester-1.8.jar",
            "META-INF/external-deps/commons-io-2.1.jar",
            "META-INF/external-deps/commons-lang-2.5.jar",
            "META-INF/external-deps/commons-logging-1.1.1.jar",
            "META-INF/external-deps/commons-math-2.1.jar",
            "META-INF/external-deps/commons-net-3.1.jar",
            "META-INF/external-deps/guava-11.0.2.jar",
            "META-INF/external-deps/hadoop-annotations-2.0.0-cdh4.2.0.jar",
            "META-INF/external-deps/hadoop-auth-2.0.0-cdh4.2.0.jar",
            "META-INF/external-deps/hadoop-common-2.0.0-cdh4.2.0.jar",
            "META-INF/external-deps/hadoop-hdfs-2.0.0-cdh4.2.0.jar",
            "META-INF/external-deps/jackson-core-asl-1.8.8.jar",
            "META-INF/external-deps/jackson-mapper-asl-1.8.8.jar",
            "META-INF/external-deps/jline-0.9.94.jar",
            "META-INF/external-deps/jsch-0.1.42.jar",
            "META-INF/external-deps/kfs-0.3.jar",
            "META-INF/external-deps/log4j-1.2.17.jar",
            "META-INF/external-deps/paranamer-2.3.jar",
            "META-INF/external-deps/protobuf-java-2.4.0a.jar",
            "META-INF/external-deps/slf4j-api-1.6.1.jar",
            "META-INF/external-deps/slf4j-log4j12-1.6.1.jar",
            "META-INF/external-deps/snappy-java-1.0.4.1.jar",
            "META-INF/external-deps/xmlenc-0.52.jar",
            "META-INF/external-deps/zookeeper-3.4.5-cdh4.2.0.jar"
    }),
    V2("2.0.0", "META-INF/plugins/hdfs-plugin-v2-0.0.1.jar", new String[] {
            "META-INF/external-deps/commons-logging-1.1.1.jar",
            "META-INF/external-deps/commons-lang-2.5.jar",
            "META-INF/external-deps/commons-configuration-1.6.jar",
            "META-INF/external-deps/commons-cli-1.2.jar",
            "META-INF/external-deps/slf4j-log4j12-1.6.1.jar",
            "META-INF/external-deps/slf4j-api-1.6.1.jar",
            "META-INF/external-deps/log4j-1.2.16.jar",
            "META-INF/external-deps/protobuf-java-2.4.0a.jar",
            "META-INF/external-deps/guava-11.0.2.jar",
            "META-INF/external-deps/hadoop-auth-2.0.2-alpha-gphd-2.0.1.jar",
            "META-INF/external-deps/hadoop-annotations-2.0.2-alpha-gphd-2.0.1.jar",
            "META-INF/external-deps/hadoop-common-2.0.2-alpha-gphd-2.0.1.jar",
            "META-INF/external-deps/hadoop-hdfs-2.0.2-alpha-gphd-2.0.1.jar"
    })
    ;

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
