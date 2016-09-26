package org.threadly.concurrent.wrapper.traceability;

import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest;

@SuppressWarnings({ "javadoc", "deprecation" })
public class ThreadRenamingSubmitterSchedulerWrapperTest extends SubmitterSchedulerInterfaceTest {
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new ThreadRenamingPoolWrapperFactory();
  }
  
  private static class ThreadRenamingPoolWrapperFactory implements SubmitterSchedulerFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();

    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }
    
    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      SubmitterScheduler scheduler = schedulerFactory.makeSubmitterScheduler(poolSize, prestartIfAvailable);
      
      return new ThreadRenamingSubmitterSchedulerWrapper(scheduler, "foo", false);
    }

    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
