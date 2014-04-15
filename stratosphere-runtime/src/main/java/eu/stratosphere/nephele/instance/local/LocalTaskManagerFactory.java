package eu.stratosphere.nephele.instance.local;

import eu.stratosphere.nephele.taskmanager.TaskManager;

public class LocalTaskManagerFactory implements TaskManagerFactory {
	@Override
	public TaskManager create(int number, int numTaskManagerPerJVM) {
		try{
			return new TaskManager(numTaskManagerPerJVM);
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
