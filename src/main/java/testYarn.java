import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.List;

/**
 * Created by qls on 7/16/15.
 */

public class testYarn {

     public static void main(String[] args){
            getJobInfo();
     }

    private static void getJobInfo() {

        Configuration conf = new YarnConfiguration();
        conf.set("yarn.resourcemanager.address", "172.16.1.81");
        YarnClient yc =new YarnClientImpl();
        yc.init(conf);
        yc.start();
        try{
          List<ApplicationReport> lists =  yc.getApplications();
          for(ApplicationReport ap : lists){
              YarnApplicationState state = ap.getYarnApplicationState();
              if("RUNNING".endsWith(state.name().toString())){
                  ApplicationId applicationId = ap.getApplicationId();
                  long startTime = ap.getStartTime();
                  long finishTime = ap.getFinishTime();
                  FinalApplicationStatus finalStatus = ap.getFinalApplicationStatus();
                  float progress =ap.getProgress();
                  System.out.println("ApplicationId "+ applicationId.toString());
                  System.out.println("YarnApplicationState " +state.name());
                  System.out.println("StartTime" + startTime);
                  System.out.println("FinishTime " + finishTime);
                  System.out.println("FinalApplicationStatus "+ finalStatus.name());
                  System.out.println("progress "+progress);
              }

          }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
