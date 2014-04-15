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

import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.example.java.wordcount.WordCount;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.test.testdata.WordCountData;
import eu.stratosphere.test.util.JavaProgramTestBase;
import org.junit.Ignore;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@Ignore
public class TaskManagerFaultToleranceITCase extends JavaProgramTestBase {
	private String textPath;
	private String resultPath;

	private final ExecutionState failingState = ExecutionState.RUNNING;
	private final Set<Integer> failingTaskManagers = new HashSet<Integer>(Arrays.asList(2));
	private final int numTaskManager = 3;
	private final int maxFailures = 2;

	public TaskManagerFaultToleranceITCase(){
		this.config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, numTaskManager);
		setDegreeOfParallelism(3);
	}

	@Override
	protected NepheleMiniCluster createCluster() {
		return new FailingNepheleMiniCluster(failingTaskManagers, failingState, maxFailures);
	}

	@Override
	protected void preSubmit() throws Exception {
		textPath = createTempFile("text.txt", WordCountData.TEXT);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(WordCountData.COUNTS_AS_TUPLES, resultPath);
	}

	@Override
	protected void testProgram() throws Exception {
		WordCount.main(new String[]{textPath, resultPath});
	}
}
