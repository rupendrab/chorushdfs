package com.emc.greenplum.hadoop.plugin;

import com.emc.greenplum.hadoop.Hdfs;
import com.emc.greenplum.hadoop.HdfsVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.PrintStream;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 8/9/12
 * Time: 11:24 AM
 * To change this template use File | Settings | File Templates.
 */
public class IntegrationTest {
    @Before
    public void setUp() throws Exception {
        Hdfs.setLoggerStream(new PrintStream(new File("/dev/null")));
        Hdfs.timeout = 1;
    }

    @After
    public void tearDown() throws Exception {
        Hdfs.timeout = 5;
    }

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

    @Test
    public void testFindNonExistantServerVersion() throws Exception {
        HdfsVersion version = Hdfs.getServerVersion("this.doesnt.exist.com", "1234", "root");
        assertNull(version);
    }
}
