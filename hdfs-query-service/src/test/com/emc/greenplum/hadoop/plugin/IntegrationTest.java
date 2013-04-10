package com.emc.greenplum.hadoop.plugin;

import com.emc.greenplum.hadoop.Hdfs;
import com.emc.greenplum.hadoop.HdfsVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;
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
    public void testCDH4Plugin() throws Exception {
        Hdfs hdfs = new Hdfs("192.168.141.132", "8020", "root", HdfsVersion.VCDH4);
        assertEquals(HdfsVersion.VCDH4, hdfs.getVersion());
        assertNotSame(0, hdfs.list("/").size());
    }

    @Test
    public void testCDH4PluginTwo() throws Exception {
        Hdfs hdfs = new Hdfs("192.168.141.132", "8020", "root");
        assertEquals(HdfsVersion.VCDH4, hdfs.getVersion());
        assertNotSame(0, hdfs.list("/").size());
    }

    /*
    @Test
    public void testMapRPlugin() throws Exception {
        Hdfs hdfs = new Hdfs("chorus-gpmr12.sf.pivotallabs.com", "7222", "root");
        assertEquals(HdfsVersion.V0202MAPR, hdfs.getVersion());
        assertNotSame(0, hdfs.list("/").size());
    }

    @Test
    public void testGphd11Plugin() throws Exception {
        Hdfs hdfs = new Hdfs("chorus-gphd11.sf.pivotallabs.com", "8020", "root");
        assertEquals(HdfsVersion.V1, hdfs.getVersion());
        assertNotSame(0, hdfs.list("/").size());
    }

    @Test
    public void testGphd12Plugin() throws Exception {
        Hdfs hdfs = new Hdfs("chorus-gphd12.sf.pivotallabs.com", "8020", "root");
        assertEquals(HdfsVersion.V1, hdfs.getVersion());
        assertNotSame(0, hdfs.list("/").size());
    }

    @Test
    public void testGphd20Plugin() throws Exception {
        Hdfs hdfs = new Hdfs("chorus-gphd20-1.sf.pivotallabs.com", "9000", "root");
        assertEquals(HdfsVersion.V2, hdfs.getVersion());
        assertNotSame(0, hdfs.list("/").size());
    }

    @Test
    public void testGphd02Plugin() throws Exception {
        Hdfs hdfs = new Hdfs("chorus-gphd02.sf.pivotallabs.com", "8020", "root");
        assertEquals(HdfsVersion.V0201GP, hdfs.getVersion());
        assertNotSame(0, hdfs.list("/").size());
    }

    @Test
    public void testVersionChange() throws Exception {
        Hdfs hdfs = new Hdfs("chorus-gphd02.sf.pivotallabs.com", "8020", "root", HdfsVersion.V1);
        assertEquals(HdfsVersion.V0201GP, hdfs.getVersion());
        assertNotSame(0, hdfs.list("/").size());
    }

    @Test
    public void testFindNonExistantServerVersion() throws Exception {
        Hdfs hdfs = new Hdfs("this.doesnt.exist.com", "1234", "root");
        assertNull(hdfs.getVersion());
    }
    */
}
