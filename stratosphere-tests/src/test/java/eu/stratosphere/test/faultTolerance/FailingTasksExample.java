package eu.stratosphere.test.faultTolerance;


import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

public class FailingTasksExample {

	public static final class Power2 extends MapFunction<Integer, Integer>{
		static int numFailures = 0;

		@Override
		public Integer map(Integer value) throws Exception {
			if(value == 0 && numFailures <= 3){
				numFailures++;
				throw new IndexOutOfBoundsException("Task with value: " + value + " failed.");
			}
			return value*value;
		}
	}

	public static void main(String[] args) throws Exception{
		int maxValue = Integer.valueOf(args[0]);
		String output = args[1];

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		List<Integer> values = new ArrayList<Integer>();

		for(int i=0; i< maxValue; i++){
			values.add(i);
		}

		DataSet<Integer> input = env.fromCollection(values);
		DataSet<Integer> squared = input.map(new Power2());
		squared.writeAsText(output);

		env.execute("Failing Tasks");
	}
}
