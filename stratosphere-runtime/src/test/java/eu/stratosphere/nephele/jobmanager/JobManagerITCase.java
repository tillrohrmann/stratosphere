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

package eu.stratosphere.nephele.jobmanager;

import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.jobgraph.*;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordSerializerFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DataSinkTask;
import eu.stratosphere.pact.runtime.task.DataSourceTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.runtime.io.channels.ChannelType;
import eu.stratosphere.nephele.jobmanager.JobManager.ExecutionMode;
import eu.stratosphere.nephele.taskmanager.Task;
import eu.stratosphere.nephele.taskmanager.TaskManager;
import eu.stratosphere.nephele.util.JarFileCreator;
import eu.stratosphere.nephele.util.ServerTestUtils;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.StringUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This test is intended to cover the basic functionality of the {@link JobManager}.
 */
public class JobManagerITCase {

	static {
		// initialize loggers
		Logger root = Logger.getRootLogger();
		root.removeAllAppenders();
		PatternLayout layout = new PatternLayout("%d{HH:mm:ss,SSS} %-5p %-60c %x - %m%n");
		ConsoleAppender appender = new ConsoleAppender(layout, "System.err");
		root.addAppender(appender);
		root.setLevel(Level.WARN);
	}
	
	
	/**
	 * The name of the test directory some tests read their input from.
	 */
	private static final String INPUT_DIRECTORY = "testDirectory";

	private static JobManagerThread jobManagerThread = null;

	private static Configuration configuration;

	/**
	 * This is an auxiliary class to run the job manager thread.
	 */
	private static final class JobManagerThread extends Thread {

		/**
		 * The job manager instance.
		 */
		private final JobManager jobManager;

		/**
		 * Constructs a new job manager thread.
		 * 
		 * @param jobManager
		 *        the job manager to run in this thread.
		 */
		private JobManagerThread(JobManager jobManager) {
			this.jobManager = jobManager;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void run() {
			// Run task loop
			this.jobManager.runTaskLoop();

			// Shut down
			this.jobManager.shutdown();
		}

		/**
		 * Checks whether the encapsulated job manager is completely shut down.
		 * 
		 * @return <code>true</code> if the encapsulated job manager is completely shut down, <code>false</code>
		 *         otherwise
		 */
		public boolean isShutDown() {
			return this.jobManager.isShutDown();
		}
	}

	/**
	 * Sets up Nephele in local mode.
	 */
	@BeforeClass
	public static void startNephele() {

		GlobalConfiguration.loadConfiguration(ServerTestUtils.getConfigDir());

		if (jobManagerThread == null) {

			// create the job manager
			JobManager jobManager;

			try {
				jobManager = new JobManager(ExecutionMode.LOCAL);
			}
			catch (Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
				return;
			}

			configuration = GlobalConfiguration.getConfiguration(new String[] { ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY });

			// Start job manager thread
			if (jobManager != null) {
				jobManagerThread = new JobManagerThread(jobManager);
				jobManagerThread.start();
			}

			// Wait for the local task manager to arrive
			try {
				ServerTestUtils.waitForJobManagerToBecomeReady(jobManager);
			} catch (Exception e) {
				fail(StringUtils.stringifyException(e));
			}
		}
	}

	/**
	 * Shuts Nephele down.
	 */
	@AfterClass
	public static void stopNephele() {

		if (jobManagerThread != null) {
			jobManagerThread.interrupt();

			while (!jobManagerThread.isShutDown()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException i) {
					break;
				}
			}
		}
	}
	
	/**
	 * Tests the correctness of the union record reader with non-empty inputs.
	 */
	@Test
	public void testUnionWithNonEmptyInput() {
		testUnion(1000000);
	}

	/**
	 * Tests of the Nephele channels with a large (> 1 MB) file.
	 */
	@Test
	public void testExecutionWithLargeInputFile() {
		test(1000000);
	}

	/**
	 * Tests of the Nephele channels with a file of zero bytes size.
	 */
	@Test
	public void testExecutionWithZeroSizeInputFile() {
		test(0);
	}

	/**
	 * Tests the execution of a job with a directory as input. The test directory contains files of different length.
	 */
	@Test
	public void testExecutionWithDirectoryInput() {

		// Define size of input
		final int sizeOfInput = 100;

		// Create test directory
		final String testDirectory = ServerTestUtils.getTempDir() + File.separator + INPUT_DIRECTORY;
		final File td = new File(testDirectory);
		if (!td.exists()) {
			td.mkdir();
		}

		File inputFile1 = null;
		File inputFile2 = null;
		File outputFile = null;
		File jarFile = null;
		JobClient jobClient = null;

		try {
			// Get name of the forward class
			final String forwardClassName = ForwardTask.class.getSimpleName();

			// Create input and jar files
			inputFile1 = ServerTestUtils.createInputFile(INPUT_DIRECTORY, 0);
			inputFile2 = ServerTestUtils.createInputFile(INPUT_DIRECTORY, sizeOfInput);
			outputFile = new File(ServerTestUtils.getTempDir() + File.separator + ServerTestUtils.getRandomFilename());
			jarFile = ServerTestUtils.createJarFile(forwardClassName);

			// Create job graph
			final JobGraph jg = new JobGraph("Job Graph 1");

			// input vertex
			final JobInputVertex i1 = new JobInputVertex("Input 1", jg);
			i1.setInputClass((Class<AbstractInputTask<?>>) (Class<?>) DataSourceTask.class);
			i1.setNumberOfSubtasks(1);
			TextInputFormat inputFormat = new TextInputFormat();
			inputFormat.setFilePath(new Path(new File(testDirectory).toURI()));
			i1.setInputFormat(inputFormat);
			i1.setOutputSerializer(RecordSerializerFactory.get());
			TaskConfig config= new TaskConfig(i1.getConfiguration());
			config.addOutputShipStrategy(ShipStrategyType.FORWARD);

			// task vertex 1
			final JobTaskVertex t1 = new JobTaskVertex("Task 1", jg);
			t1.setTaskClass(ForwardTask.class);

			// task vertex 2
			final JobTaskVertex t2 = new JobTaskVertex("Task 2", jg);
			t2.setTaskClass(ForwardTask.class);

			// output vertex
			JobOutputVertex o1 = new JobOutputVertex("Output 1", jg);
			o1.setNumberOfSubtasks(1);
			o1.setOutputClass(DataSinkTask.class);
			CsvOutputFormat outputFormat = new CsvOutputFormat(StringValue.class);
			outputFormat.setOutputFilePath(new Path(outputFile.toURI()));
			o1.setOutputFormat(outputFormat);
			TaskConfig outputConfig = new TaskConfig(o1.getConfiguration());
			outputConfig.addInputToGroup(0);
			outputConfig.setInputSerializer(RecordSerializerFactory.get(), 0);

			t1.setVertexToShareInstancesWith(i1);
			t2.setVertexToShareInstancesWith(i1);
			o1.setVertexToShareInstancesWith(i1);

			// connect vertices
			try {
				i1.connectTo(t1, ChannelType.NETWORK);
				t1.connectTo(t2, ChannelType.IN_MEMORY);
				t2.connectTo(o1, ChannelType.IN_MEMORY);
			} catch (JobGraphDefinitionException e) {
				e.printStackTrace();
			}

			// add jar
			jg.addJar(new Path(new File(ServerTestUtils.getTempDir() + File.separator + forwardClassName + ".jar").toURI()));

			// Create job client and launch job
			jobClient = new JobClient(jg, configuration);
			jobClient.submitJobAndWait();

			// Finally, compare output file to initial number sequence
			final BufferedReader bufferedReader = new BufferedReader(new FileReader(outputFile));
			for (int i = 0; i < sizeOfInput; i++) {
				final String number = bufferedReader.readLine();
				try {
					assertEquals(i, Integer.parseInt(number));
				} catch (NumberFormatException e) {
					fail(e.getMessage());
				}
			}

			bufferedReader.close();

		} catch (NumberFormatException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (JobExecutionException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (IOException ioe) {
			ioe.printStackTrace();
			fail(ioe.getMessage());
		} finally {
			// Remove temporary files
			if (inputFile1 != null) {
				inputFile1.delete();
			}
			if (inputFile2 != null) {
				inputFile2.delete();
			}
			if (outputFile != null) {
				outputFile.delete();
			}
			if (jarFile != null) {
				jarFile.delete();
			}

			// Remove test directory
			if (td != null) {
				td.delete();
			}

			if (jobClient != null) {
				jobClient.close();
			}
		}
	}

	/**
	 * Tests the Nephele execution when an exception occurs. In particular, it is tested if the information that is
	 * wrapped by the exception is correctly passed on to the client.
	 */
	@Test
	public void testExecutionWithException() {

		final String exceptionClassName = ExceptionTask.class.getSimpleName();
		File inputFile = null;
		File outputFile = null;
		File jarFile = null;
		JobClient jobClient = null;

		try {

			inputFile = ServerTestUtils.createInputFile(0);
			outputFile = new File(ServerTestUtils.getTempDir() + File.separator + ServerTestUtils.getRandomFilename());
			jarFile = ServerTestUtils.createJarFile(exceptionClassName);

			// Create job graph
			final JobGraph jg = new JobGraph("Job Graph for Exception Test");

			// input vertex
			final JobInputVertex i1 = new JobInputVertex("Input 1", jg);
			i1.setNumberOfSubtasks(1);
			Class<AbstractInputTask<?>> clazz = (Class<AbstractInputTask<?>>)(Class<?>)
					DataSourceTask.class;
			i1.setInputClass(clazz);
			TextInputFormat inputFormat = new TextInputFormat();
			inputFormat.setFilePath(new Path(inputFile.toURI()));
			i1.setInputFormat(inputFormat);
			i1.setOutputSerializer(RecordSerializerFactory.get());
			TaskConfig config= new TaskConfig(i1.getConfiguration());
			config.addOutputShipStrategy(ShipStrategyType.FORWARD);

			// task vertex 1
			final JobTaskVertex t1 = new JobTaskVertex("Task with Exception", jg);
			t1.setTaskClass(ExceptionTask.class);

			// output vertex
			JobOutputVertex o1 = new JobOutputVertex("Output 1", jg);
			o1.setNumberOfSubtasks(1);
			o1.setOutputClass(DataSinkTask.class);
			CsvOutputFormat outputFormat = new CsvOutputFormat(StringValue.class);
			outputFormat.setOutputFilePath(new Path(outputFile.toURI()));
			o1.setOutputFormat(outputFormat);
			TaskConfig outputConfig = new TaskConfig(o1.getConfiguration());
			outputConfig.addInputToGroup(0);
			outputConfig.setInputSerializer(RecordSerializerFactory.get(), 0);

			t1.setVertexToShareInstancesWith(i1);
			o1.setVertexToShareInstancesWith(i1);

			// connect vertices
			i1.connectTo(t1, ChannelType.IN_MEMORY);
			t1.connectTo(o1, ChannelType.IN_MEMORY);

			// add jar
			jg.addJar(new Path(new File(ServerTestUtils.getTempDir() + File.separator + exceptionClassName + ".jar")
				.toURI()));

			// Create job client and launch job
			jobClient = new JobClient(jg, configuration);
			
			// deactivate logging of expected test exceptions
			Logger rtLogger = Logger.getLogger(Task.class);
			Level rtLevel = rtLogger.getEffectiveLevel();
			rtLogger.setLevel(Level.OFF);
			
			try {
				jobClient.submitJobAndWait();
			} catch (JobExecutionException e) {
				// Check if the correct error message is encapsulated in the exception
				if (e.getMessage() == null) {
					fail("JobExecutionException does not contain an error message");
				}
				if (!e.getMessage().contains(ExceptionTask.ERROR_MESSAGE)) {
					fail("JobExecutionException does not contain the expected error message");
				}

				return;
			}
			finally {
				rtLogger.setLevel(rtLevel);
			}

			fail("Expected exception but did not receive it");

		} catch (JobGraphDefinitionException jgde) {
			fail(jgde.getMessage());
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		} finally {

			// Remove temporary files
			if (inputFile != null) {
				inputFile.delete();
			}
			if (outputFile != null) {
				outputFile.delete();
			}
			if (jarFile != null) {
				jarFile.delete();
			}

			if (jobClient != null) {
				jobClient.close();
			}
		}
	}

	/**
	 * Tests the Nephele execution when a runtime exception during the registration of the input/output gates occurs.
	 */
	@Test
	public void testExecutionWithRuntimeException() {

		final String runtimeExceptionClassName = RuntimeExceptionTask.class.getSimpleName();
		File inputFile = null;
		File outputFile = null;
		File jarFile = null;
		JobClient jobClient = null;

		try {

			inputFile = ServerTestUtils.createInputFile(100);
			outputFile = new File(ServerTestUtils.getTempDir() + File.separator + ServerTestUtils.getRandomFilename());
			jarFile = ServerTestUtils.createJarFile(runtimeExceptionClassName);

			// Create job graph
			final JobGraph jg = new JobGraph("Job Graph for Exception Test");

			// input vertex
			final JobInputVertex i1 = new JobInputVertex("Input 1", jg);
			i1.setNumberOfSubtasks(1);
			Class<AbstractInputTask<?>> clazz = (Class<AbstractInputTask<?>>)(Class<?>)DataSourceTask
					.class;
			i1.setInputClass(clazz);
			TextInputFormat inputFormat = new TextInputFormat();
			inputFormat.setFilePath(new Path(inputFile.toURI()));
			i1.setInputFormat(inputFormat);
			i1.setInputFormat(inputFormat);
			i1.setOutputSerializer(RecordSerializerFactory.get());
			TaskConfig config= new TaskConfig(i1.getConfiguration());
			config.addOutputShipStrategy(ShipStrategyType.FORWARD);

			// task vertex 1
			final JobTaskVertex t1 = new JobTaskVertex("Task with Exception", jg);
			t1.setTaskClass(RuntimeExceptionTask.class);

			// output vertex
			JobOutputVertex o1 = new JobOutputVertex("Output 1", jg);
			o1.setNumberOfSubtasks(1);
			o1.setOutputClass(DataSinkTask.class);
			CsvOutputFormat outputFormat = new CsvOutputFormat(StringValue.class);
			outputFormat.setOutputFilePath(new Path(outputFile.toURI()));
			o1.setOutputFormat(outputFormat);
			TaskConfig outputConfig = new TaskConfig(o1.getConfiguration());
			outputConfig.addInputToGroup(0);
			outputConfig.setInputSerializer(RecordSerializerFactory.get(), 0);

			t1.setVertexToShareInstancesWith(i1);
			o1.setVertexToShareInstancesWith(i1);

			// connect vertices
			i1.connectTo(t1, ChannelType.IN_MEMORY);
			t1.connectTo(o1, ChannelType.IN_MEMORY);

			// add jar
			jg.addJar(new Path(new File(ServerTestUtils.getTempDir() + File.separator + runtimeExceptionClassName
				+ ".jar").toURI()));

			// Create job client and launch job
			jobClient = new JobClient(jg, configuration);
			
			// deactivate logging of expected test exceptions
			Logger jcLogger = Logger.getLogger(JobClient.class);
			Level jcLevel = jcLogger.getEffectiveLevel();
			jcLogger.setLevel(Level.OFF);
			try {
				jobClient.submitJobAndWait();
			} catch (JobExecutionException e) {

				// Check if the correct error message is encapsulated in the exception
				if (e.getMessage() == null) {
					fail("JobExecutionException does not contain an error message");
				}
				if (!e.getMessage().contains(RuntimeExceptionTask.RUNTIME_EXCEPTION_MESSAGE)) {
					fail("JobExecutionException does not contain the expected error message, " +
							"but instead: " + e.getMessage());
				}

				// Check if the correct error message is encapsulated in the exception
				return;
			}
			finally {
				jcLogger.setLevel(jcLevel);
			}

			fail("Expected exception but did not receive it");

		} catch (JobGraphDefinitionException jgde) {
			fail(jgde.getMessage());
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		} finally {

			// Remove temporary files
			if (inputFile != null) {
				inputFile.delete();
			}
			if (outputFile != null) {
				outputFile.delete();
			}
			if (jarFile != null) {
				jarFile.delete();
			}

			if (jobClient != null) {
				jobClient.close();
			}
		}
	}

	/**
	 * Tests the Nephele execution when a runtime exception in the output format occurs.
	 */
	@Test
	public void testExecutionWithRuntimeExceptionInOutputFormat() {

		final String runtimeExceptionClassName = RuntimeExceptionTask.class.getSimpleName();
		File inputFile = null;
		File outputFile = null;
		File jarFile = null;
		JobClient jobClient = null;

		try {

			inputFile = ServerTestUtils.createInputFile(100);
			outputFile = new File(ServerTestUtils.getTempDir() + File.separator + ServerTestUtils.getRandomFilename());
			jarFile = ServerTestUtils.createJarFile(runtimeExceptionClassName);

			// Create job graph
			final JobGraph jg = new JobGraph("Job Graph for Exception Test");

			// input vertex
			final JobInputVertex i1 = new JobInputVertex("Input 1", jg);
			i1.setNumberOfSubtasks(1);
			Class<AbstractInputTask<?>> clazz = (Class<AbstractInputTask<?>>)(Class<?>)DataSourceTask
					.class;
			i1.setInputClass(clazz);
			TextInputFormat inputFormat = new TextInputFormat();
			inputFormat.setFilePath(new Path(inputFile.toURI()));
			i1.setInputFormat(inputFormat);
			i1.setInputFormat(inputFormat);
			i1.setOutputSerializer(RecordSerializerFactory.get());
			TaskConfig config= new TaskConfig(i1.getConfiguration());
			config.addOutputShipStrategy(ShipStrategyType.FORWARD);

			// task vertex 1
			final JobTaskVertex t1 = new JobTaskVertex("Task with Exception", jg);
			t1.setTaskClass(ForwardTask.class);

			// output vertex
			JobOutputVertex o1 = new JobOutputVertex("Output 1", jg);
			o1.setNumberOfSubtasks(1);
			o1.setOutputClass(DataSinkTask.class);
			ExceptionOutputFormat outputFormat = new ExceptionOutputFormat();
			o1.setOutputFormat(outputFormat);
			TaskConfig outputConfig = new TaskConfig(o1.getConfiguration());
			outputConfig.addInputToGroup(0);
			outputConfig.setInputSerializer(RecordSerializerFactory.get(), 0);

			t1.setVertexToShareInstancesWith(i1);
			o1.setVertexToShareInstancesWith(i1);

			// connect vertices
			i1.connectTo(t1, ChannelType.IN_MEMORY);
			t1.connectTo(o1, ChannelType.IN_MEMORY);

			// add jar
			jg.addJar(new Path(new File(ServerTestUtils.getTempDir() + File.separator + runtimeExceptionClassName
					+ ".jar").toURI()));

			// Create job client and launch job
			jobClient = new JobClient(jg, configuration);

			// deactivate logging of expected test exceptions
			Logger jcLogger = Logger.getLogger(JobClient.class);
			Level jcLevel = jcLogger.getEffectiveLevel();
			jcLogger.setLevel(Level.OFF);
			try {
				jobClient.submitJobAndWait();
			} catch (JobExecutionException e) {

				// Check if the correct error message is encapsulated in the exception
				if (e.getMessage() == null) {
					fail("JobExecutionException does not contain an error message");
				}
				if (!e.getMessage().contains(RuntimeExceptionTask.RUNTIME_EXCEPTION_MESSAGE)) {
					fail("JobExecutionException does not contain the expected error message, " +
							"but instead: " + e.getMessage());
				}

				// Check if the correct error message is encapsulated in the exception
				return;
			}
			finally {
				jcLogger.setLevel(jcLevel);
			}

			fail("Expected exception but did not receive it");

		} catch (JobGraphDefinitionException jgde) {
			fail(jgde.getMessage());
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		} finally {

			// Remove temporary files
			if (inputFile != null) {
				inputFile.delete();
			}
			if (outputFile != null) {
				outputFile.delete();
			}
			if (jarFile != null) {
				jarFile.delete();
			}

			if (jobClient != null) {
				jobClient.close();
			}
		}
	}

	/**
	 * Creates a file with a sequence of 0 to <code>limit</code> integer numbers
	 * and triggers a sample job. The sample reads all the numbers from the input file and pushes them through a
	 * network, a file, and an in-memory channel. Eventually, the numbers are written back to an output file. The test
	 * is considered successful if the input file equals the output file.
	 * 
	 * @param limit
	 *        the upper bound for the sequence of numbers to be generated
	 */
	private void test(final int limit) {

		JobClient jobClient = null;

		try {

			// Get name of the forward class
			final String forwardClassName = ForwardTask.class.getSimpleName();

			// Create input and jar files
			final File inputFile = ServerTestUtils.createInputFile(limit);
			final File outputFile = new File(ServerTestUtils.getTempDir() + File.separator
				+ ServerTestUtils.getRandomFilename());
			final File jarFile = ServerTestUtils.createJarFile(forwardClassName);

			// Create job graph
			final JobGraph jg = new JobGraph("Job Graph 1");

			// input vertex
			final JobInputVertex i1 = new JobInputVertex("Input 1", jg);
			i1.setNumberOfSubtasks(1);
			Class<AbstractInputTask<?>> clazz = (Class<AbstractInputTask<?>>)(Class<?>)DataSourceTask
					.class;
			i1.setInputClass(clazz);
			TextInputFormat inputFormat = new TextInputFormat();
			inputFormat.setFilePath(new Path(inputFile.toURI()));
			i1.setInputFormat(inputFormat);
			i1.setOutputSerializer(RecordSerializerFactory.get());
			TaskConfig config= new TaskConfig(i1.getConfiguration());
			config.addOutputShipStrategy(ShipStrategyType.FORWARD);

			// task vertex 1
			final JobTaskVertex t1 = new JobTaskVertex("Task 1", jg);
			t1.setTaskClass(ForwardTask.class);

			// task vertex 2
			final JobTaskVertex t2 = new JobTaskVertex("Task 2", jg);
			t2.setTaskClass(ForwardTask.class);

			// output vertex
			JobOutputVertex o1 = new JobOutputVertex("Output 1", jg);
			o1.setNumberOfSubtasks(1);
			o1.setOutputClass(DataSinkTask.class);
			CsvOutputFormat outputFormat = new CsvOutputFormat(StringValue.class);
			outputFormat.setOutputFilePath(new Path(outputFile.toURI()));
			o1.setOutputFormat(outputFormat);
			TaskConfig outputConfig = new TaskConfig(o1.getConfiguration());
			outputConfig.addInputToGroup(0);
			outputConfig.setInputSerializer(RecordSerializerFactory.get(),0);


			t1.setVertexToShareInstancesWith(i1);
			t2.setVertexToShareInstancesWith(i1);
			o1.setVertexToShareInstancesWith(i1);

			// connect vertices
			try {
				i1.connectTo(t1, ChannelType.NETWORK);
				t1.connectTo(t2, ChannelType.IN_MEMORY);
				t2.connectTo(o1, ChannelType.IN_MEMORY);
			} catch (JobGraphDefinitionException e) {
				e.printStackTrace();
			}

			// add jar
			jg.addJar(new Path(new File(ServerTestUtils.getTempDir() + File.separator + forwardClassName + ".jar")
				.toURI()));

			// Create job client and launch job
			jobClient = new JobClient(jg, configuration);
			
			try {
				jobClient.submitJobAndWait();
			} catch (JobExecutionException e) {
				fail(e.getMessage());
			}

			// Finally, compare output file to initial number sequence
			final BufferedReader bufferedReader = new BufferedReader(new FileReader(outputFile));
			for (int i = 0; i < limit; i++) {
				final String number = bufferedReader.readLine();
				try {
					assertEquals(i, Integer.parseInt(number));
				} catch (NumberFormatException e) {
					fail(e.getMessage());
				}
			}

			bufferedReader.close();

			// Remove temporary files
			inputFile.delete();
			outputFile.delete();
			jarFile.delete();

		} catch (IOException ioe) {
			ioe.printStackTrace();
			fail(ioe.getMessage());
		} finally {
			if (jobClient != null) {
				jobClient.close();
			}
		}
	}

	/**
	 * Tests the Nephele execution with a job that has two vertices, that are connected twice with each other with
	 * different channel types.
	 */
	@Test
	public void testExecutionDoubleConnection() {

		File inputFile = null;
		File outputFile = null;
		File jarFile = new File(ServerTestUtils.getTempDir() + File.separator + "doubleConnection.jar");
		JobClient jobClient = null;

		try {

			inputFile = ServerTestUtils.createInputFile(0);
			outputFile = new File(ServerTestUtils.getTempDir() + File.separator + ServerTestUtils.getRandomFilename());

			// Create required jar file
			JarFileCreator jfc = new JarFileCreator(jarFile);
			jfc.addClass(DoubleTargetTask.class);
			jfc.createJarFile();

			// Create job graph
			final JobGraph jg = new JobGraph("Job Graph for Double Connection Test");

			// input vertex
			final JobInputVertex i1 = new JobInputVertex("Input 1", jg);
			i1.setNumberOfSubtasks(1);
			Class<AbstractInputTask<?>> clazz = (Class<AbstractInputTask<?>>)(Class<?>)DataSourceTask
					.class;
			i1.setInputClass(clazz);
			TextInputFormat inputFormat = new TextInputFormat();
			inputFormat.setFilePath(new Path(inputFile.toURI()));
			i1.setInputFormat(inputFormat);
			i1.setOutputSerializer(RecordSerializerFactory.get());
			TaskConfig config= new TaskConfig(i1.getConfiguration());
			config.addOutputShipStrategy(ShipStrategyType.FORWARD);
			config.addOutputShipStrategy(ShipStrategyType.BROADCAST);


			// task vertex 1
			final JobTaskVertex t1 = new JobTaskVertex("Task with two Inputs", jg);
			t1.setTaskClass(DoubleTargetTask.class);

			// output vertex
			JobOutputVertex o1 = new JobOutputVertex("Output 1", jg);
			o1.setNumberOfSubtasks(1);
			o1.setOutputClass(DataSinkTask.class);
			CsvOutputFormat outputFormat = new CsvOutputFormat(StringValue.class);
			outputFormat.setOutputFilePath(new Path(outputFile.toURI()));
			o1.setOutputFormat(outputFormat);
			TaskConfig outputConfig = new TaskConfig(o1.getConfiguration());
			outputConfig.addInputToGroup(0);
			outputConfig.setInputSerializer(RecordSerializerFactory.get(), 0);


			t1.setVertexToShareInstancesWith(i1);
			o1.setVertexToShareInstancesWith(i1);

			// connect vertices
			i1.connectTo(t1, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);
			i1.connectTo(t1, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
			t1.connectTo(o1, ChannelType.IN_MEMORY);

			// add jar
			jg.addJar(new Path(jarFile.toURI()));

			// Create job client and launch job
			jobClient = new JobClient(jg, configuration);
			jobClient.submitJobAndWait();

		} catch (JobExecutionException e) {
			fail(e.getMessage());
		} catch (JobGraphDefinitionException jgde) {
			fail(jgde.getMessage());
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		} finally {

			// Remove temporary files
			if (inputFile != null) {
				inputFile.delete();
			}
			if (outputFile != null) {
				outputFile.delete();
			}
			if (jarFile != null) {
				jarFile.delete();
			}

			if (jobClient != null) {
				jobClient.close();
			}
		}
	}

	/**
	 * Tests the Nephele job execution when the graph and the tasks are given no specific name.
	 */
	@Test
	public void testEmptyTaskNames() {

		File inputFile = null;
		File outputFile = null;
		JobClient jobClient = null;

		try {

			inputFile = ServerTestUtils.createInputFile(0);
			outputFile = new File(ServerTestUtils.getTempDir() + File.separator + ServerTestUtils.getRandomFilename());

			// Create job graph
			final JobGraph jg = new JobGraph();

			// input vertex
			final JobInputVertex i1 = new JobInputVertex("Input 1", jg);
			i1.setNumberOfSubtasks(1);
			Class<AbstractInputTask<?>> clazz = (Class<AbstractInputTask<?>>)(Class<?>)DataSourceTask
					.class;
			i1.setInputClass(clazz);
			TextInputFormat inputFormat = new TextInputFormat();
			inputFormat.setFilePath(new Path(inputFile.toURI()));
			i1.setInputFormat(inputFormat);
			i1.setOutputSerializer(RecordSerializerFactory.get());
			TaskConfig config= new TaskConfig(i1.getConfiguration());
			config.addOutputShipStrategy(ShipStrategyType.FORWARD);

			// output vertex
			JobOutputVertex o1 = new JobOutputVertex("Output 1", jg);
			o1.setNumberOfSubtasks(1);
			o1.setOutputClass(DataSinkTask.class);
			CsvOutputFormat outputFormat = new CsvOutputFormat(StringValue.class);
			outputFormat.setOutputFilePath(new Path(outputFile.toURI()));
			o1.setOutputFormat(outputFormat);
			TaskConfig outputConfig = new TaskConfig(o1.getConfiguration());
			outputConfig.addInputToGroup(0);
			outputConfig.setInputSerializer(RecordSerializerFactory.get(), 0);

			o1.setVertexToShareInstancesWith(i1);

			// connect vertices
			i1.connectTo(o1, ChannelType.IN_MEMORY);

			// Create job client and launch job
			jobClient = new JobClient(jg, configuration);
			jobClient.submitJobAndWait();

		} catch (JobExecutionException e) {
			fail(e.getMessage());
		} catch (JobGraphDefinitionException jgde) {
			fail(jgde.getMessage());
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		} finally {

			// Remove temporary files
			if (inputFile != null) {
				inputFile.delete();
			}
			if (outputFile != null) {
				outputFile.delete();
			}

			if (jobClient != null) {
				jobClient.close();
			}
		}
	}

	/**
	 * Tests the correctness of the union record reader with empty inputs.
	 */
	@Test
	public void testUnionWithEmptyInput() {
		testUnion(0);
	}

	/**
	 * Tests the correctness of the union reader for different input sizes.
	 * 
	 * @param limit
	 *        the upper bound for the sequence of numbers to be generated
	 */
	private void testUnion(final int limit) {

		File inputFile1 = null;
		File inputFile2 = null;
		File outputFile = null;
		File jarFile = new File(ServerTestUtils.getTempDir() + File.separator + "unionWithEmptyInput.jar");
		JobClient jobClient = null;

		try {

			inputFile1 = ServerTestUtils.createInputFile(limit);
			inputFile2 = ServerTestUtils.createInputFile(limit);
			outputFile = new File(ServerTestUtils.getTempDir() + File.separator + ServerTestUtils.getRandomFilename());

			// Create required jar file
			JarFileCreator jfc = new JarFileCreator(jarFile);
			jfc.addClass(UnionTask.class);
			jfc.createJarFile();

			// Create job graph
			final JobGraph jg = new JobGraph("Union job " + limit);

			// input vertex
			final JobInputVertex i1 = new JobInputVertex("Input 1", jg);
			i1.setNumberOfSubtasks(1);
			Class<AbstractInputTask<?>> clazz = (Class<AbstractInputTask<?>>)(Class<?>)DataSourceTask
					.class;
			i1.setInputClass(clazz);
			TextInputFormat inputFormat1 = new TextInputFormat();
			inputFormat1.setFilePath(new Path(inputFile1.toURI()));
			i1.setInputFormat(inputFormat1);
			i1.setOutputSerializer(RecordSerializerFactory.get());
			TaskConfig config1= new TaskConfig(i1.getConfiguration());
			config1.addOutputShipStrategy(ShipStrategyType.FORWARD);

			// input vertex
			final JobInputVertex i2 = new JobInputVertex("Input 2", jg);
			i2.setNumberOfSubtasks(1);
			i2.setInputClass(clazz);
			TextInputFormat inputFormat2 = new TextInputFormat();
			inputFormat2.setFilePath(new Path(inputFile2.toURI()));
			i2.setInputFormat(inputFormat2);
			i2.setOutputSerializer(RecordSerializerFactory.get());
			TaskConfig config2 = new TaskConfig(i2.getConfiguration());
			config2.addOutputShipStrategy(ShipStrategyType.FORWARD);

			// union task
			final JobTaskVertex u1 = new JobTaskVertex("Union", jg);
			u1.setTaskClass(UnionTask.class);

			// output vertex
			JobOutputVertex o1 = new JobOutputVertex("Output 1", jg);
			o1.setNumberOfSubtasks(1);
			o1.setOutputClass(DataSinkTask.class);
			CsvOutputFormat outputFormat = new CsvOutputFormat(StringValue.class);
			outputFormat.setOutputFilePath(new Path(outputFile.toURI()));
			o1.setOutputFormat(outputFormat);
			TaskConfig outputConfig = new TaskConfig(o1.getConfiguration());
			outputConfig.addInputToGroup(0);
			outputConfig.setInputSerializer(RecordSerializerFactory.get(),0);

			i1.setVertexToShareInstancesWith(o1);
			i2.setVertexToShareInstancesWith(o1);
			u1.setVertexToShareInstancesWith(o1);

			// connect vertices
			i1.connectTo(u1, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);
			i2.connectTo(u1, ChannelType.IN_MEMORY);
			u1.connectTo(o1, ChannelType.IN_MEMORY);

			// add jar
			jg.addJar(new Path(jarFile.toURI()));

			// Create job client and launch job
			jobClient = new JobClient(jg, configuration);

			try {
				jobClient.submitJobAndWait();
			} catch (JobExecutionException e) {
				fail(e.getMessage());
			}

			// Finally, check the output
			final Map<Integer, Integer> expectedNumbers = new HashMap<Integer, Integer>();
			final Integer two = Integer.valueOf(2);
			for (int i = 0; i < limit; ++i) {
				expectedNumbers.put(Integer.valueOf(i), two);
			}

			final BufferedReader bufferedReader = new BufferedReader(new FileReader(outputFile));
			String line = bufferedReader.readLine();
			while (line != null) {

				final Integer number = Integer.valueOf(Integer.parseInt(line));
				Integer val = expectedNumbers.get(number);
				if (val == null) {
					fail("Found unexpected number in union output: " + number);
				} else {
					val = Integer.valueOf(val.intValue() - 1);
					if (val.intValue() < 0) {
						fail(val + " occurred more than twice in union output");
					}
					if (val.intValue() == 0) {
						expectedNumbers.remove(number);
					} else {
						expectedNumbers.put(number, val);
					}
				}

				line = bufferedReader.readLine();
			}

			bufferedReader.close();

			if (!expectedNumbers.isEmpty()) {
				final StringBuilder str = new StringBuilder();
				str.append("The following numbers have not been found in the union output:\n");
				final Iterator<Map.Entry<Integer, Integer>> it = expectedNumbers.entrySet().iterator();
				while (it.hasNext()) {
					final Map.Entry<Integer, Integer> entry = it.next();
					str.append(entry.getKey().toString());
					str.append(" (");
					str.append(entry.getValue().toString());
					str.append("x)\n");
				}

				fail(str.toString());
			}

		} catch (JobGraphDefinitionException jgde) {
			fail(jgde.getMessage());
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		} finally {

			// Remove temporary files
			if (inputFile1 != null) {
				inputFile1.delete();
			}
			if (inputFile2 != null) {
				inputFile2.delete();
			}
			if (outputFile != null) {
				outputFile.delete();
			}
			if (jarFile != null) {
				jarFile.delete();
			}

			if (jobClient != null) {
				jobClient.close();
			}
		}
	}

	/**
	 * Tests the execution of a job with a large degree of parallelism. In particular, the tests checks that the overall
	 * runtime of the test does not exceed a certain time limit.
	 */
	@Test
	public void testExecutionWithLargeDoP() {

		// The degree of parallelism to be used by tasks in this job.
		final int numberOfSubtasks = 64;

		File inputFile1 = null;
		File inputFile2 = null;
		File outputFile = null;
		File jarFile = new File(ServerTestUtils.getTempDir() + File.separator + "largeDoP.jar");
		JobClient jobClient = null;

		try {

			inputFile1 = ServerTestUtils.createInputFile(0);
			inputFile2 = ServerTestUtils.createInputFile(0);
			outputFile = new File(ServerTestUtils.getTempDir() + File.separator + ServerTestUtils.getRandomFilename());

			// Create required jar file
			JarFileCreator jfc = new JarFileCreator(jarFile);
			jfc.addClass(UnionTask.class);
			jfc.createJarFile();

			// Create job graph
			final JobGraph jg = new JobGraph("Job with large DoP (" + numberOfSubtasks + ")");

			// input vertex
			final JobInputVertex i1 = new JobInputVertex("Input 1", jg);
			Class<AbstractInputTask<?>> clazz = (Class<AbstractInputTask<?>>)(Class<?>)DataSourceTask
					.class;
			i1.setInputClass(clazz);
			TextInputFormat inputFormat1 = new TextInputFormat();
			inputFormat1.setFilePath(new Path(inputFile1.toURI()));
			i1.setInputFormat(inputFormat1);
			i1.setNumberOfSubtasks(numberOfSubtasks);
			i1.setNumberOfSubtasksPerInstance(numberOfSubtasks);
			i1.setOutputSerializer(RecordSerializerFactory.get());
			TaskConfig config= new TaskConfig(i1.getConfiguration());
			config.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);

			// input vertex
			final JobInputVertex i2 = new JobInputVertex("Input 2", jg);
			i2.setInputClass(clazz);
			TextInputFormat inputFormat2 = new TextInputFormat();
			inputFormat2.setFilePath(new Path(inputFile2.toURI()));
			i2.setInputFormat(inputFormat2);
			i2.setNumberOfSubtasks(numberOfSubtasks);
			i2.setNumberOfSubtasksPerInstance(numberOfSubtasks);
			i2.setOutputSerializer(RecordSerializerFactory.get());
			TaskConfig config2 = new TaskConfig(i2.getConfiguration());
			config2.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);

			// union task
			final JobTaskVertex f1 = new JobTaskVertex("Forward 1", jg);
			f1.setTaskClass(DoubleTargetTask.class);
			f1.setNumberOfSubtasks(numberOfSubtasks);
			f1.setNumberOfSubtasksPerInstance(numberOfSubtasks);

			// output vertex
			JobOutputVertex o1 = new JobOutputVertex("Output 1", jg);
			o1.setOutputClass(DataSinkTask.class);
			CsvOutputFormat outputFormat = new CsvOutputFormat(StringValue.class);
			outputFormat.setOutputFilePath(new Path(outputFile.toURI()));
			o1.setOutputFormat(outputFormat);
			o1.setNumberOfSubtasks(numberOfSubtasks);
			o1.setNumberOfSubtasksPerInstance(numberOfSubtasks);
			TaskConfig outputConfig = new TaskConfig(o1.getConfiguration());
			outputConfig.addInputToGroup(0);
			outputConfig.setInputSerializer(RecordSerializerFactory.get(),0);

			i1.setVertexToShareInstancesWith(o1);
			i2.setVertexToShareInstancesWith(o1);
			f1.setVertexToShareInstancesWith(o1);

			// connect vertices
			i1.connectTo(f1, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
			i2.connectTo(f1, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
			f1.connectTo(o1, ChannelType.NETWORK, DistributionPattern.BIPARTITE);

			// add jar
			jg.addJar(new Path(jarFile.toURI()));

			// Create job client and launch job
			jobClient = new JobClient(jg, configuration);
			
			// disable logging for the taskmanager and the client, as they will have many
			// expected test errors they will log.
			
			Logger tmLogger = Logger.getLogger(TaskManager.class);
			Logger jcLogger = Logger.getLogger(JobClient.class);
			Level tmLevel = tmLogger.getEffectiveLevel();
			Level jcLevel = jcLogger.getEffectiveLevel();
			
			tmLogger.setLevel(Level.OFF);
			jcLogger.setLevel(Level.OFF);
			try {
				
				jobClient.submitJobAndWait();
			} catch (JobExecutionException e) {
				// Job execution should lead to an error due to lack of resources
				return;
			}
			finally {
				tmLogger.setLevel(tmLevel);
				jcLogger.setLevel(jcLevel);
			}

			fail("Undetected lack of resources");

		} catch (JobGraphDefinitionException jgde) {
			fail(jgde.getMessage());
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		} finally {

			// Remove temporary files
			if (inputFile1 != null) {
				inputFile1.delete();
			}
			if (inputFile2 != null) {
				inputFile2.delete();
			}
			if (outputFile != null) {
				if (outputFile.isDirectory()) {
					final String[] files = outputFile.list();
					final String outputDir = outputFile.getAbsolutePath();
					for (final String file : files) {
						new File(outputDir + File.separator + file).delete();
					}
				}
				outputFile.delete();
			}
			if (jarFile != null) {
				jarFile.delete();
			}

			if (jobClient != null) {
				jobClient.close();
			}
		}
	}
}
