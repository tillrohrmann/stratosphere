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
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.TaskManager;

import java.util.HashMap;
import java.util.Map;

public class FailingTaskManager extends TaskManager {
	private static Map<Integer, Integer> failures = new HashMap<Integer, Integer>();
	private final int id;
	private final boolean failing;
	private final ExecutionState failingState;
	private final int maxFailures;

	public FailingTaskManager(int id, int numTaskManagerPerJVM, boolean failing,
							  ExecutionState failingState, int maxFailures) throws Exception{
		super(numTaskManagerPerJVM);
		this.id = id;
		this.failing = failing;
		this.failingState = failingState;
		this.maxFailures = maxFailures;
		failures.put(id, 0);
	}

	@Override
	public void executionStateChanged(final JobID jobID, final ExecutionVertexID vertexID,
									  final ExecutionState newExecutionState, final String optionalDescription) {
		super.executionStateChanged(jobID, vertexID, newExecutionState, optionalDescription);

		if(failing && failingState == newExecutionState && failures.get(id) < maxFailures){
			failures.put(id, failures.get(id) + 1);

			throw new RuntimeException("TaskManager #" + this.id + " failed.");
		}

	}
}
