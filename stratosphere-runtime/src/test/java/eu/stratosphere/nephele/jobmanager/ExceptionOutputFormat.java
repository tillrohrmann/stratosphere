package eu.stratosphere.nephele.jobmanager;

import eu.stratosphere.api.common.io.OutputFormat;
import eu.stratosphere.configuration.Configuration;

import java.io.IOException;

/**
 * Created by till on 27/03/14.
 */
public class ExceptionOutputFormat implements OutputFormat<Object> {
	/**
	 * The message which is used for the test runtime exception.
	 */
	public static final String RUNTIME_EXCEPTION_MESSAGE = "This is a test runtime exception";


	@Override
	public void configure(Configuration parameters) {

	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {

	}

	@Override
	public void writeRecord(Object record) throws IOException {

	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public void initialize(Configuration configuration) {
		throw new RuntimeException(RUNTIME_EXCEPTION_MESSAGE);
	}
}
