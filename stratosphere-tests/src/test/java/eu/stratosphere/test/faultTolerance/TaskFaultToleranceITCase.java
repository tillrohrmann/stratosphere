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

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.test.util.JavaProgramTestBase;
import org.junit.Ignore;

@Ignore
public class TaskFaultToleranceITCase extends JavaProgramTestBase{
	private final int numInputs = 10;
	private String outputPath;

	public TaskFaultToleranceITCase(){
		this.config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 3);
		setDegreeOfParallelism(3);
	}

	@Override
	protected void preSubmit() throws Exception {
		outputPath = this.getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		FailingTasksExample.main(new String[]{numInputs +"", outputPath});
	}
}
