package com.emc.greenplum.hadoop.plugin;

import com.emc.greenplum.hadoop.Hdfs;
import com.emc.greenplum.hadoop.HdfsVersion;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 8/9/12
 * Time: 11:24 AM
 * To change this template use File | Settings | File Templates.
 */
public class IntegrationTest {
    @Test
    public void testMapRPlugin() throws Exception {
        HdfsVersion version = Hdfs.getServerVersion("chorus-gpmr12.sf.pivotallabs.com", "7222", "root");
        Hdfs hdfs = new Hdfs("chorus-gpmr12.sf.pivotallabs.com", "7222", "root", version);
        assertNotSame(0, hdfs.list("/").size());
    }

    @Test
    public void testGphd11Plugin() throws Exception {
        HdfsVersion version = Hdfs.getServerVersion("chorus-gphd11.sf.pivotallabs.com", "8020", "root");
        Hdfs hdfs = new Hdfs("chorus-gphd11.sf.pivotallabs.com", "8020", "root", version);
        assertNotSame(0, hdfs.list("/").size());
    }

    @Test
    public void testGphd02Plugin() throws Exception {
        HdfsVersion version = Hdfs.getServerVersion("chorus-gphd02.sf.pivotallabs.com", "8020", "root");
        Hdfs hdfs = new Hdfs("chorus-gphd02.sf.pivotallabs.com", "8020", "root", version);
        assertNotSame(0, hdfs.list("/").size());
    }
}
