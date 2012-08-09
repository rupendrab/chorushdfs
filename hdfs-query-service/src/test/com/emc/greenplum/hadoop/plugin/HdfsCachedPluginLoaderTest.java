package test.com.emc.greenplum.hadoop.plugin;

import com.emc.greenplum.hadoop.plugin.HdfsCachedPluginLoader;
import com.emc.greenplum.hadoop.HdfsVersion;
import com.emc.greenplum.hadoop.plugin.HdfsPluginBuilder;
import com.emc.greenplum.hadoop.plugin.HdfsPluginLoader;
import com.emc.greenplum.hadoop.plugins.HdfsFileSystem;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 8/8/12
 * Time: 6:18 PM
 * To change this template use File | Settings | File Templates.
 */
public class HdfsCachedPluginLoaderTest {

    private HdfsPluginBuilder builder;
    private HdfsPluginLoader pluginLoaderMock;

    @Before
    public void setUp() throws Exception {
        builder = mock(HdfsPluginBuilder.class);
        pluginLoaderMock = mock(HdfsPluginLoader.class);
        when(builder.build(HdfsVersion.V1)).thenReturn(pluginLoaderMock);
    }

    @Test
    public void testLoadPluginWhenNewVersion() throws Exception {
        new HdfsCachedPluginLoader(builder).loadPlugin(HdfsVersion.V1);
        verify(builder).build(HdfsVersion.V1);
    }

    @Test
    public void testLoadPluginFromCache() {
        HdfsCachedPluginLoader hdfsCachedPluginLoader = new HdfsCachedPluginLoader(builder);
        HdfsFileSystem first = hdfsCachedPluginLoader.loadPlugin(HdfsVersion.V1);
        verify(builder).build(HdfsVersion.V1);

        reset(builder);
        HdfsFileSystem second = hdfsCachedPluginLoader.loadPlugin(HdfsVersion.V1);
        verifyZeroInteractions(builder);

        assertEquals(first, second);
    }
}