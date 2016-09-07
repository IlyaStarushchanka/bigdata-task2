package com.epam.bigdata.yarnapp;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by Ilya_Starushchanka on 9/6/2016.
 */
public class ApplicationMasterNew implements AMRMClientAsync.CallbackHandler{

    private Configuration configuration;
    private NMClient nmClient;
    private String command;
    private int numContainersToWaitFor;
    private AtomicInteger currentContainer = new AtomicInteger(0);
    private int linesCount;
    private String inputFile;



    public ApplicationMasterNew(String inputFile, int numContainersToWaitFor) {
        this.inputFile = inputFile;
        configuration = new YarnConfiguration();
        this.numContainersToWaitFor = numContainersToWaitFor;
        nmClient = NMClient.createNMClient();
        nmClient.init(configuration);
        nmClient.start();
    }

    public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            System.out.println("[AM] onContainersAllocated count: " + currentContainer.incrementAndGet());
            try {
                // Launch container by create ContainerLaunchContext
                int offset, count;
                if (currentContainer.intValue() < numContainersToWaitFor){
                    count = Math.round(linesCount/numContainersToWaitFor);
                } else {
                    count = linesCount - Math.round(linesCount/numContainersToWaitFor)*(numContainersToWaitFor - 1);
                }

                offset = Math.round(linesCount/numContainersToWaitFor)*(currentContainer.intValue() - 1);

                ContainerLaunchContext ctx =
                        Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(
                        Collections.singletonList(
                                "$JAVA_HOME/bin/java" +
                                        " -Xms256M -Xmx512M" +
                                        " com.epam.bigdata.yarnapp.WordCount" +
                                        " " + inputFile +
                                        " " + offset +
                                        " " + count +
                                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                        ));
                Map<String, String> containerEnv = new HashMap<>();
                containerEnv.put("CLASSPATH", "./*");
                ctx.setEnvironment(containerEnv);

                LocalResource appMasterJar = Records.newRecord(LocalResource.class);
                setupAppMasterJar(Constants.HDFS_MY_APP_JAR_PATH, appMasterJar);
                ctx.setLocalResources(Collections.singletonMap("simple-app.jar", appMasterJar));
                System.out.println("[AM] Launching container " + container.getId());
                nmClient.startContainer(container, ctx);
            } catch (Exception ex) {
                System.err.println("[AM] Error launching container " + container.getId() + " " + ex);
            }
        }
    }

    private void setupAppMasterJar(Path jarPath, LocalResource appMasterJar) throws IOException {
        FileStatus jarStat = FileSystem.get(getConfiguration()).getFileStatus(jarPath);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
    }

    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            System.out.println("[AM] Completed container " + status.getContainerId());
            synchronized (this) {
                numContainersToWaitFor--;
            }
        }
    }

    public void onNodesUpdated(List<NodeReport> updated) {
    }

    public void onReboot() {
    }

    public void onShutdownRequest() {
    }

    public void onError(Throwable t) {
    }

    public float getProgress() {
        return 0;
    }

    public boolean doneWithContainers() {
        return numContainersToWaitFor == 0;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        final String inputFile = args[0];
        final int n = Integer.valueOf(args[1]);

        ApplicationMasterNew master = new ApplicationMasterNew(inputFile, n);
        master.runMainLoop();

    }

    public void runMainLoop() throws Exception {

        linesCount = FileHelper.getLinesCount(inputFile);

        AMRMClientAsync<ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(getConfiguration());
        rmClient.start();

        // Register with ResourceManager
        System.out.println("[AM] registerApplicationMaster 0");
        rmClient.registerApplicationMaster("", 0, "");
        System.out.println("[AM] registerApplicationMaster 1");

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        // Make container requests to ResourceManager
        for (int i = 0; i < numContainersToWaitFor; ++i) {
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            System.out.println("[AM] Making res-req " + i);
            rmClient.addContainerRequest(containerAsk);
        }

        System.out.println("[AM] waiting for containers to finish");
        while (!doneWithContainers()) {
            Thread.sleep(100);
        }

        System.out.println("[AM] unregisterApplicationMaster 0");
        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
        System.out.println("[AM] unregisterApplicationMaster 1");
    }
}
