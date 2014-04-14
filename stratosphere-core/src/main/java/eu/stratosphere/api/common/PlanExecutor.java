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

package eu.stratosphere.api.common;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * A PlanExecutor runs a plan. The specific implementation (such as the {@link eu.stratosphere.client.LocalExecutor}
 * and {@link eu.stratosphere.client.RemoteExecutor}) determines where and how to run the plan. 
 */
public abstract class PlanExecutor {

	private static final String LOCAL_EXECUTOR_CLASS = "eu.stratosphere.client.LocalExecutor";
	private static final String REMOTE_EXECUTOR_CLASS = "eu.stratosphere.client.RemoteExecutor";
	
	
	/**
	 * Execute the given plan and return the runtime in milliseconds.
	 * 
	 * @param plan The plan of the program to execute.
	 * @return The execution result, containing for example the net runtime of the program, and the accumulators.
	 * 
	 * @throws Exception Thrown, i job submission caused an exception.
	 */
	public abstract JobExecutionResult executePlan(Plan plan) throws Exception;
	
	
	public abstract String getOptimizerPlanAsJSON(Plan plan) throws Exception;
	
	
	
	
	
	public static PlanExecutor createLocalExecutor(int numTaskManager) {
		Class<? extends PlanExecutor> leClass = loadExecutorClass(LOCAL_EXECUTOR_CLASS);
		
		try {
			return leClass.getConstructor(int.class).newInstance(numTaskManager);
		}
		catch (Throwable t) {
			throw new RuntimeException("An error occurred while loading the local executor (" + LOCAL_EXECUTOR_CLASS + ").", t);
		}
	}

	public static PlanExecutor createRemoteExecutor(String hostname, int port, String... jarFiles) {
		if (hostname == null) {
			throw new IllegalArgumentException("The hostname must not be null.");
		}
		if (port <= 0 || port > Short.MAX_VALUE) {
			throw new IllegalArgumentException("The port value is out of range.");
		}
		
		Class<? extends PlanExecutor> leClass = loadExecutorClass(REMOTE_EXECUTOR_CLASS);
		
		List<String> files = (jarFiles == null || jarFiles.length == 0) ? Collections.<String>emptyList() : Arrays.asList(jarFiles); 
		
		try {
			return leClass.getConstructor(String.class, int.class, List.class).newInstance(hostname, port, files);
		}
		catch (Throwable t) {
			throw new RuntimeException("An error occurred while loading the remote executor (" + REMOTE_EXECUTOR_CLASS
					+ ").", t);
		}
	}
	
	
	private static final Class<? extends PlanExecutor> loadExecutorClass(String className) {
		try {
			Class<?> leClass = Class.forName(LOCAL_EXECUTOR_CLASS);
			return leClass.asSubclass(PlanExecutor.class);
		}
		catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("Could not load the executor class (" + className + "). Do you have the 'stratosphere-clients' project in your dependencies?");
		}
		catch (Throwable t) {
			throw new RuntimeException("An error occurred while loading the executor (" + className + ").", t);
		}
	}
}