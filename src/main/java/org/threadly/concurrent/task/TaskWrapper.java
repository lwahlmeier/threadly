package org.threadly.concurrent.task;

import org.threadly.concurrent.RunnableContainer;
import org.threadly.concurrent.TaskPriority;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;

/**
 * Abstract implementation for all tasks handled by this pool.
 * 
 * @since 1.0.0
 */
public class TaskWrapper implements RunnableContainer {
  private final Runnable task;
  private final boolean isFixedRate;
  private final long reRunDelay;
  private final TaskPriority priority;
  private volatile long nextRunTime;
  private volatile boolean invalidated;
  private volatile boolean executing;
  private volatile short executeFlipCounter = 0;
  
  public TaskWrapper(Runnable task, TaskPriority priority) {
    this(task, priority, Clock.lastKnownForwardProgressingMillis(), -1L, false);
  }
  
  public TaskWrapper(Runnable task, TaskPriority priority, long startTime) {
    this(task, priority, startTime, -1L, false);
  }
  
  public TaskWrapper(Runnable task, TaskPriority priority, long startTime, long reRunDelay, boolean isFixedRate) {
    this.task = task;
    this.reRunDelay = reRunDelay;
    this.isFixedRate = isFixedRate;
    this.priority = priority;
    nextRunTime = startTime;
    invalidated = false;
//    System.out.println("Task:"+runDelay+":"+reRunDelay+":"+nextRunTime);
  }
  
  /**
   * Similar to {@link Runnable#run()}, this is invoked to execute the contained task.  One 
   * critical difference is this implementation should never throw an exception (even 
   * {@link RuntimeException}'s).  Throwing such an exception would result in the worker thread 
   * dying (and being leaked from the pool).
   */
  public void runTask() {
    if (! invalidated) {
      ExceptionUtils.runRunnable(task);
    }
  }
  
  /**
   * Attempts to invalidate the task from running (assuming it has not started yet).  If the 
   * task is recurring then future executions will also be avoided.
   */
  public void invalidate() {
    invalidated = true;
  }
  
  /**
   * Get an execution reference so that we can ensure thread safe access into 
   * {@link #canExecute(short)}.
   * 
   * @return Short to identify execution state 
   */
  public short getExecuteReference() {
    return executeFlipCounter;
  }
  
  /**
   * Get the absolute time when this should run, in comparison with the time returned from 
   * {@link org.threadly.util.Clock#accurateForwardProgressingMillis()}.
   * 
   * @return Absolute time in millis this task should run
   */
  public long getRunTime() {
    if (executing) {
      return Long.MAX_VALUE;
    } else {
      return nextRunTime;
    }
  }
  
  public TaskPriority getPriority() {
    return this.priority;
  }
  
  public boolean isRecurring() {
    return reRunDelay >= 0;
  }
  
  public boolean isFixedRate() {
    return reRunDelay >= 0 && this.isFixedRate;
  }
  
  /**
   * Simple getter for the run time, this is expected to do NO operations for calculating the 
   * run time.  The main reason this is used over {@link #getRunTime()} is to allow the JVM to 
   * jit the function better.  Because of the nature of this, this can only be used at very 
   * specific points in the tasks lifecycle, and can not be used for sorting operations.
   * 
   * @return An un-molested representation of the stored absolute run time
   */
  public long getPureRunTime() {
    return nextRunTime;
  }
  
  /**
   * Call to see how long the task should be delayed before execution.  While this may return 
   * either positive or negative numbers, only an accurate number is returned if the task must 
   * be delayed for execution.  If the task is ready to execute it may return zero even though 
   * it is past due.  For that reason you can NOT use this to compare two tasks for execution 
   * order, instead you should use {@link #getRunTime()}.
   * 
   * @return delay in milliseconds till task can be run
   */
  public long getScheduleDelay() {
    if (getRunTime() > Clock.lastKnownForwardProgressingMillis()) {
      return getRunTime() - Clock.accurateForwardProgressingMillis();
    } else {
      return 0;
    }
  }
  
  @Override
  public String toString() {
    return task.toString();
  }
  
  @Override
  public Runnable getContainedRunnable() {
    return task;
  }
  
  public void updateNextRunTime() {
    nextRunTime = Clock.accurateForwardProgressingMillis() + reRunDelay;
  }
}