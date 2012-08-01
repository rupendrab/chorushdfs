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
    public static void main(String[] args) {
        try {
            HdfsVersion version = Hdfs.getServerVersion("chorus-gpmr12.sf.pivotallabs.com", "7222", "root");
            System.out.println(version.getName());
            Hdfs hdfs = new Hdfs("chorus-gpmr12.sf.pivotallabs.com", "7222", "root", version);
            hdfs.list("/");
        } catch (Exception e) {
            System.out.println("Failed to connect to MapR!!!");
            e.printStackTrace();
        }

        try {
            HdfsVersion version = Hdfs.getServerVersion("chorus-gphd11.sf.pivotallabs.com", "8020", "root");
            System.out.println(version.getName());
            Hdfs hdfs = new Hdfs("chorus-gphd11.sf.pivotallabs.com", "8020", "root", version);
            hdfs.list("/");
        } catch (Exception e) {
            System.out.println("Failed to connect to GPHD 1.1!!!");
            e.printStackTrace();
        }

        try {
            HdfsVersion version = Hdfs.getServerVersion("chorus-gphd02.sf.pivotallabs.com", "8020", "root");
            System.out.println(version.getName());
            Hdfs hdfs = new Hdfs("chorus-gphd02.sf.pivotallabs.com", "8020", "root", version);
            hdfs.list("/");
        } catch (Exception e) {
            System.out.println("Failed to connect to GPHD 0.2!!!");
            e.printStackTrace();
        }
    }
}
