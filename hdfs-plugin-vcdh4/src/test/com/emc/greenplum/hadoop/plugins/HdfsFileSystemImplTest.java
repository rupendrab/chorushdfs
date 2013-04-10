package test.com.emc.greenplum.hadoop.plugins;
import com.emc.greenplum.hadoop.plugins.HdfsFileSystem;
import com.emc.greenplum.hadoop.plugins.HdfsFileSystemImpl;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 2/28/13
 * Time: 2:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class HdfsFileSystemImplTest {
    @Test
    public void testHdfsFileSystemImpl() {
        HdfsFileSystem hdfs = new HdfsFileSystemImpl();
        hdfs.loadFileSystem("192.168.141.132", "8020", "cloudera");
        assertNotNull(hdfs);
    }
}

