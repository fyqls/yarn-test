import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

/**
 * Created by zs on 15/3/9.
 */
public class YarnDemo {
  private static final String HA_NAMESERVICE_ADD = "hdfs://nameservice1";
  private static final String NAMESERVICES = "nameservice1";
  private static final String NAMESERVICE_NODE1 = "1";
  private static final String NAMESERVICE_NODE2 = "2";

  private static final String NODE1_ADD = "tw-node1204:8020";
  private static final String NODE2_ADD = "tw-node1205:8020";

  public void testConnect() {
    YarnClient  yarnClient = null;
    try {
      yarnClient = getYarnClient(getYarnConf());
      List<NodeReport> nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
      for (NodeReport node : nodeReports) {
        String httpaddress = node.getHttpAddress();
        System.out.println("running node: " + node.getNodeId() + " " + httpaddress.substring(0, httpaddress.indexOf(":")
        ) + " " + node.getRackName() + " " + node.getHealthReport());
      }

      List<NodeReport> failReports = yarnClient.getNodeReports(NodeState.LOST);
      List<NodeReport> nodes = yarnClient.getNodeReports(NodeState.UNHEALTHY);
      for (NodeReport node : failReports) {
        System.out.println("fail node: " + node.getHttpAddress());
      }

      System.out.println(yarnClient.getServiceState());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (yarnClient != null) {
        try {
          yarnClient.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private YarnClient getYarnClient(Configuration conf) throws IOException, InterruptedException {
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI("yarn/tw-node2110@TDH",
        "/etc/yarn1/yarn.keytab");
    YarnClient client = ugi.doAs(new PrivilegedExceptionAction<YarnClient>() {
      @Override
      public YarnClient run() throws IOException {
        return YarnClient.createYarnClient();
      }
    });
    client.init(conf);
    client.start();
    return client;
  }

  private Configuration getYarnConf() {
    Configuration conf = new YarnConfiguration();
//    conf.set("fs.defaultFS", "hdfs://tw-node1204:8020");
//    conf.set("mapreduce.framework.name", "yarn");
    conf.set("yarn.resourcemanager.address", "tw-node2110:8032");
    conf.set(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, "1");
    conf.set(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, "1000");
    conf.set(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, "500");

    conf.set(YarnConfiguration.RM_PRINCIPAL, "yarn/_HOST@TDH");
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    return conf;
  }

  private Configuration getYarnHAConf() {
    Configuration conf = new YarnConfiguration();
    conf.set("fs.defaultFS", HA_NAMESERVICE_ADD);
    conf.set("dfs.nameservices", NAMESERVICES);
    conf.set("dfs.ha.namenodes." + NAMESERVICES, NAMESERVICE_NODE2 + "," +  NAMESERVICE_NODE1);
    conf.set("dfs.namenode.rpc-address." + NAMESERVICES + "." + NAMESERVICE_NODE2, NODE2_ADD);
    conf.set("dfs.namenode.rpc-address." + NAMESERVICES + "." + NAMESERVICE_NODE1, NODE1_ADD);
    conf.set("dfs.client.failover.proxy.provider." + NAMESERVICES, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    conf.set("mapreduce.framework.name", "yarn");
    conf.set("yarn.resourcemanager.address", "tw-node1204:8032");
    return conf;
  }

  public static void main(String[] args) {
    YarnDemo demo = new YarnDemo();
    demo.testConnect();
  }
}
