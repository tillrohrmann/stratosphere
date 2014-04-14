/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.client.minicluster;

import java.lang.reflect.Method;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.io.FileInputFormat;
import eu.stratosphere.api.common.io.FileOutputFormat;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.jobmanager.JobManager.ExecutionMode;


public class NepheleMiniCluster {
	
	private static final Log LOG = LogFactory.getLog(NepheleMiniCluster.class);
	
	private static final int DEFAULT_JM_RPC_PORT = 6498;
	
	private static final int DEFAULT_TM_RPC_PORT = 6501;
	
	private static final int DEFAULT_TM_DATA_PORT = 7501;
	
	private static final boolean DEFAULT_VISUALIZER_ENABLED = true;

	// --------------------------------------------------------------------------------------------
	
	private final Object startStopLock = new Object();
	
	private int jobManagerRpcPort = DEFAULT_JM_RPC_PORT;
	
	private int taskManagerRpcPort = DEFAULT_TM_RPC_PORT;
	
	private int taskManagerDataPort = DEFAULT_TM_DATA_PORT;
	
	private String configDir;

	private String hdfsConfigFile;

	private boolean visualizerEnabled = DEFAULT_VISUALIZER_ENABLED;
	
	private boolean defaultOverwriteFiles = false;
	
	private boolean defaultAlwaysCreateDirectory = false;

	
	private Thread runner;

	private JobManager jobManager;

	// ------------------------------------------------------------------------
	//  Constructor and feature / properties setup
	// ------------------------------------------------------------------------

	public int getJobManagerRpcPort() {
		return jobManagerRpcPort;
	}
	
	public void setJobManagerRpcPort(int jobManagerRpcPort) {
		this.jobManagerRpcPort = jobManagerRpcPort;
	}

	public int getTaskManagerRpcPort() {
		return taskManagerRpcPort;
	}

	public void setTaskManagerRpcPort(int taskManagerRpcPort) {
		this.taskManagerRpcPort = taskManagerRpcPort;
	}

	public int getTaskManagerDataPort() {
		return taskManagerDataPort;
	}

	public void setTaskManagerDataPort(int taskManagerDataPort) {
		this.taskManagerDataPort = taskManagerDataPort;
	}
	
	public String getConfigDir() {
		return configDir;
	}

	public void setConfigDir(String configDir) {
		this.configDir = configDir;
	}

	public String getHdfsConfigFile() {
		return hdfsConfigFile;
	}
	
	public void setHdfsConfigFile(String hdfsConfigFile) {
		this.hdfsConfigFile = hdfsConfigFile;
	}
	
	public boolean isVisualizerEnabled() {
		return visualizerEnabled;
	}
	
	public void setVisualizerEnabled(boolean visualizerEnabled) {
		this.visualizerEnabled = visualizerEnabled;
	}
	
	public boolean isDefaultOverwriteFiles() {
		return defaultOverwriteFiles;
	}
	
	public void setDefaultOverwriteFiles(boolean defaultOverwriteFiles) {
		this.defaultOverwriteFiles = defaultOverwriteFiles;
	}
	
	public boolean isDefaultAlwaysCreateDirectory() {
		return defaultAlwaysCreateDirectory;
	}
	
	public void setDefaultAlwaysCreateDirectory(boolean defaultAlwaysCreateDirectory) {
		this.defaultAlwaysCreateDirectory = defaultAlwaysCreateDirectory;
	}

	
	// ------------------------------------------------------------------------
	// Life cycle and Job Submission
	// ------------------------------------------------------------------------
	
	public JobClient getJobClient(JobGraph jobGraph) throws Exception {
		Configuration configuration = jobGraph.getJobConfiguration();
		configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
		configuration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerRpcPort);
		return new JobClient(jobGraph, configuration);
	}

	public void start() throws Exception{
		start(1);
	}
	
	public void start(int numTaskManagers) throws Exception {
		synchronized (startStopLock) {
			// set up the global configuration
			if (this.configDir != null) {
				GlobalConfiguration.loadConfiguration(configDir);
			} else {
				Configuration conf = getMiniclusterDefaultConfig(jobManagerRpcPort, taskManagerRpcPort,
					taskManagerDataPort, hdfsConfigFile, visualizerEnabled, defaultOverwriteFiles, defaultAlwaysCreateDirectory);
				GlobalConfiguration.includeConfiguration(conf);
			}
			
			// force the input/output format classes to load the default values from the configuration.
			// we need to do this here, because the format classes may have been initialized before the mini cluster was started
			initializeIOFormatClasses();
			
			// before we start the jobmanager, we need to make sure that there are no lingering IPC threads from before
			// check that all threads are done before we return
			Thread[] allThreads = new Thread[Thread.activeCount()];
			int numThreads = Thread.enumerate(allThreads);
			
			for (int i = 0; i < numThreads; i++) {
				Thread t = allThreads[i];
				String name = t.getName();
				if (name.equals("Local Taskmanager IO Loop") || name.startsWith("IPC")) {
					t.join();
				}
			}

			Configuration tmConf = new Configuration();
			tmConf.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, numTaskManagers);
			GlobalConfiguration.includeConfiguration(tmConf);
			
			// start the job manager
			jobManager = new JobManager(ExecutionMode.LOCAL);
			runner = new Thread("JobManager Task Loop") {
				@Override
				public void run() {
					// run the main task loop
					jobManager.runTaskLoop();
				}
			};
			runner.setDaemon(true);
			runner.start();
	
			waitForJobManagerToBecomeReady(numTaskManagers);
		}
	}

	public void stop() throws Exception {
		synchronized (this.startStopLock) {
			if (jobManager != null) {
				jobManager.shutdown();
				jobManager = null;
			}
	
			if (runner != null) {
				runner.interrupt();
				runner.join();
				runner = null;
			}
		}
	}

	// ------------------------------------------------------------------------
	// Network utility methods
	// ------------------------------------------------------------------------
	
	private void waitForJobManagerToBecomeReady(int numTaskManagers) throws InterruptedException {
		while (jobManager.getNumberOfTaskTrackers() < numTaskManagers) {
			Thread.sleep(100);
		}
	}
	
	private static void initializeIOFormatClasses() {
		try {
			Method im = FileInputFormat.class.getDeclaredMethod("initDefaultsFromConfiguration");
			im.setAccessible(true);
			im.invoke(null);
			
			Method om = FileOutputFormat.class.getDeclaredMethod("initDefaultsFromConfiguration");
			om.setAccessible(true);
			om.invoke(null);
		}
		catch (Exception e) {
			LOG.error("Cannot (re) initialize the globally loaded defaults. Some classes might mot follow the specified default behavior.");
		}
	}
	
	public static Configuration getMiniclusterDefaultConfig(int jobManagerRpcPort, int taskManagerRpcPort,
			int taskManagerDataPort, String hdfsConfigFile, boolean visualization,
			boolean defaultOverwriteFiles, boolean defaultAlwaysCreateDirectory)
	{
		final Configuration config = new Configuration();
		
		// addresses and ports
		config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
		config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerRpcPort);
		config.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, taskManagerRpcPort);
		config.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, taskManagerDataPort);
		
		// with the low dop, we can use few RPC handlers
		config.setInteger(ConfigConstants.JOB_MANAGER_IPC_HANDLERS_KEY, 2);
		
		// polling interval
		config.setInteger(ConfigConstants.JOBCLIENT_POLLING_INTERVAL_KEY, 2);
		
		// enable / disable features
		config.setBoolean("jobmanager.visualization.enable", visualization);
		
		// hdfs
		if (hdfsConfigFile != null) {
			config.setString(ConfigConstants.HDFS_DEFAULT_CONFIG, hdfsConfigFile);
		}
		
		// file system behavior
		config.setBoolean(ConfigConstants.FILESYSTEM_DEFAULT_OVERWRITE_KEY, defaultOverwriteFiles);
		config.setBoolean(ConfigConstants.FILESYSTEM_OUTPUT_ALWAYS_CREATE_DIRECTORY_KEY, defaultAlwaysCreateDirectory);

		return config;
	}
}