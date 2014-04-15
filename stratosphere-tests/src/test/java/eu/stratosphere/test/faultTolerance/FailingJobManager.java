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

package eu.stratosphere.test.faultTolerance;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.jobmanager.JobManager;

import java.util.Set;

public class FailingJobManager extends JobManager {

	private final Set<Integer> failingTaskManagers;
	private final ExecutionState failingState;
	private final int maxFailures;

	public FailingJobManager(Set<Integer> failingTaskManagers, ExecutionState failingState, int maxFailures){
		this.failingTaskManagers = failingTaskManagers;
		this.failingState = failingState;
		this.maxFailures = maxFailures;
	}

	@Override
	protected InstanceManager loadInstanceManager(ExecutionMode executionMode){
		switch(executionMode){
			case LOCAL:
				return new FailingLocalInstanceManager(new FailingTaskManagerFactory(failingTaskManagers,
						failingState, maxFailures));
			case CLUSTER:
				break;
		}
		throw new RuntimeException("FailingJobManager does not support cluster execution mode.");
	}
}
