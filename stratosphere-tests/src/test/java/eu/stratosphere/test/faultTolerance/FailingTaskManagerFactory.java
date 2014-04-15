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
import eu.stratosphere.nephele.instance.local.TaskManagerFactory;
import eu.stratosphere.nephele.taskmanager.TaskManager;

import java.util.Set;

public class FailingTaskManagerFactory implements TaskManagerFactory {
	private final Set<Integer> failingTaskManager;
	private final ExecutionState failingState;
	private final int maxFailures;

	public FailingTaskManagerFactory(Set<Integer> failingTaskManager, ExecutionState failingState, int maxFailures){
		this.failingTaskManager = failingTaskManager;
		this.failingState = failingState;
		this.maxFailures = maxFailures;
	}

	@Override
	public TaskManager create(int number, int numTaskManagerPerJVM) {
		try{
			return new FailingTaskManager(number, numTaskManagerPerJVM, failingTaskManager.contains(number),
					failingState, maxFailures);
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
