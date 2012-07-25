import com.emc.greenplum.hadoop.Hdfs;
import com.emc.greenplum.hadoop.HdfsVersion;
import com.emc.greenplum.hadoop.plugins.HdfsEntity;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 7/24/12
 * Time: 2:42 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestConnect {
    public static void main(String [] args) {
        Hdfs hdfs = new Hdfs("chorus-gpmr12", "7222", "root", HdfsVersion.V0202MAPR);
        for(HdfsEntity entity: hdfs.list("/")) {
            System.out.println("Entity: "+ entity.getPath());
        }
    }
}
