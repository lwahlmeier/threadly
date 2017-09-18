package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableRunnableFuture;
import org.threadly.concurrent.task.TaskWrapper;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;
import org.threadly.util.SortUtils;

/**
 * Abstract implementation for implementations of {@link PrioritySchedulerService}.  In general 
 * this wont be useful outside of Threadly developers, but must be a public interface since it is 
 * used in sub-packages.
 * <p>
 * If you do find yourself using this class, please post an issue on github to tell us why.  If 
 * there is something you want our schedulers to provide, we are happy to hear about it.
 * 
 * @since 4.3.0
 */
public abstract class AbstractPriorityScheduler extends AbstractSubmitterScheduler 
implements PrioritySchedulerService {
  protected static final TaskPriority DEFAULT_PRIORITY = TaskPriority.High;
  protected static final int DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS = 500;
  // tuned for performance of scheduled tasks
  protected static final int QUEUE_FRONT_PADDING = 0;
  protected static final int QUEUE_REAR_PADDING = 2;
  
  protected final TaskPriority defaultPriority;
  
  protected AbstractPriorityScheduler(TaskPriority defaultPriority) {
    if (defaultPriority == null) {
      defaultPriority = DEFAULT_PRIORITY;
    }
    this.defaultPriority = defaultPriority;
  }
  
  /**
   * Changes the max wait time for low priority tasks.  This is the amount of time that a low 
   * priority task will wait if there are ready to execute high priority tasks.  After a low 
   * priority task has waited this amount of time, it will be executed fairly with high priority 
   * tasks (meaning it will only execute the high priority task if it has been waiting longer than 
   * the low priority task).
   * 
   * @param maxWaitForLowPriorityInMs new wait time in milliseconds for low priority tasks during thread contention
   */
  public void setMaxWaitForLowPriority(long maxWaitForLowPriorityInMs) {
    getQueueManager().setMaxWaitForLowPriority(maxWaitForLowPriorityInMs);
  }
  
  @Override
  public long getMaxWaitForLowPriority() {
    return getQueueManager().getMaxWaitForLowPriority();
  }
  
  @Override
  public TaskPriority getDefaultPriority() {
    return defaultPriority;
  }
  
  @Override
  protected final void doSchedule(Runnable task, long delayInMillis) {
    doSchedule(task, delayInMillis, defaultPriority);
  }
  
  /**
   * Constructs a {@link OneTimeTaskWrapper} and adds it to the most efficient queue.  If there is 
   * no delay it will use {@link #addToExecuteQueue(OneTimeTaskWrapper)}, if there is a delay it 
   * will be added to {@link #addToScheduleQueue(TaskWrapper)}.
   * 
   * @param task Runnable to be executed
   * @param delayInMillis delay to wait before task is run
   * @param priority Priority for task execution
   * @return Wrapper that was scheduled
   */
  protected abstract TaskWrapper doSchedule(Runnable task, 
                                                   long delayInMillis, 
                                                   TaskPriority priority);
  
  @Override
  public void execute(Runnable task, TaskPriority priority) {
    schedule(task, 0, priority);
  }
  
  @Override
  public ListenableFuture<?> submit(Runnable task, TaskPriority priority) {
    return submitScheduled(task, null, 0, priority);
  }
  
  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result, TaskPriority priority) {
    return submitScheduled(task, result, 0, priority);
  }
  
  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task, TaskPriority priority) {
    return submitScheduled(task, 0, priority);
  }
  
  @Override
  public void schedule(Runnable task, long delayInMs, TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(delayInMs, "delayInMs");
    if (priority == null) {
      priority = defaultPriority;
    }
    
    doSchedule(task, delayInMs, priority);
  }
  
  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs, 
                                             TaskPriority priority) {
    return submitScheduled(task, null, delayInMs, priority);
  }
  
  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, 
                                                 long delayInMs, TaskPriority priority) {
    return submitScheduled(new RunnableCallableAdapter<>(task, result), delayInMs, priority);
  }
  
  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs, 
                                                 TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(delayInMs, "delayInMs");
    if (priority == null) {
      priority = defaultPriority;
    }
    
    ListenableRunnableFuture<T> rf = new ListenableFutureTask<>(false, task);
    doSchedule(rf, delayInMs, priority);
    
    return rf;
  }
  
  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
    scheduleWithFixedDelay(task, initialDelay, recurringDelay, null);
  }
  
  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
    scheduleAtFixedRate(task, initialDelay, period, null);
  }
  
  @Override
  public boolean remove(Runnable task) {
    return getQueueManager().remove(task);
  }
  
  @Override
  public boolean remove(Callable<?> task) {
    return getQueueManager().remove(task);
  }
  
  /**
   * Call to get reference to {@link QueueManager}.  This reference can be used to get access to 
   * queues directly, or perform operations which are distributed to multiple queues.  This 
   * reference can not be maintained in this abstract class to allow the potential for 
   * {@link QueueSetListener}'s which want to reference things in {@code this}.
   * 
   * @return Manager for queue sets
   */
  protected abstract QueueManager getQueueManager();
  
  @Override
  public int getQueuedTaskCount() {
    int result = 0;
    for (TaskPriority p : TaskPriority.values()) {
      result += getQueueManager().getQueueSet(p).queueSize();
    }
    return result;
  }
  
  @Override
  public int getQueuedTaskCount(TaskPriority priority) {
    if (priority == null) {
      return getQueuedTaskCount();
    }
    
    return getQueueManager().getQueueSet(priority).queueSize();
  }
  
  @Override
  public int getWaitingForExecutionTaskCount() {
    int result = 0;
    for (TaskPriority p : TaskPriority.values()) {
      result += getWaitingForExecutionTaskCount(p);
    }
    return result;
  }
  
  @Override
  public int getWaitingForExecutionTaskCount(TaskPriority priority) {
    if (priority == null) {
      return getWaitingForExecutionTaskCount();
    }
    
    QueueSet qs = getQueueManager().getQueueSet(priority);
    int result = qs.executeQueue.size();
    for (int i = 0; i < qs.scheduleQueue.size(); i++) {
      try {
        if (qs.scheduleQueue.get(i).getScheduleDelay() > 0) {
          break;
        } else {
          result++;
        }
      } catch (IndexOutOfBoundsException e) {
        break;
      }
    }
    return result;
  }
  
  /**
   * Interface to be notified when relevant changes happen to the queue set.
   * 
   * @since 4.3.0
   */
  protected interface QueueSetListener {
    /**
     * Invoked when the head of the queue set has been updated.  This can be used to wake up 
     * blocking threads waiting for tasks to consume.
     */
    public void handleQueueUpdate();
  }
  
  /**
   * Class to contain structures for both execution and scheduling.  It also contains logic for 
   * how we get and add tasks to this queue.
   * <p>
   * This allows us to have one structure for each priority.  Each structure determines what is 
   * the next task for a given priority.
   * 
   * @since 4.0.0
   */
  protected static class QueueSet {
    protected final QueueSetListener queueListener;
    protected final ConcurrentLinkedQueue<TaskWrapper> executeQueue;
    protected final ConcurrentArrayList<TaskWrapper> scheduleQueue;
    protected final Function<Integer, Long> scheduleQueueRunTimeByIndex;
    protected volatile long lastPulled = Clock.accurateForwardProgressingMillis();
    protected final AtomicBoolean checkingScheduled = new AtomicBoolean(false);
    
    public QueueSet(QueueSetListener queueListener) {
      this.queueListener = queueListener;
      this.executeQueue = new ConcurrentLinkedQueue<>();
      this.scheduleQueue = new ConcurrentArrayList<>(QUEUE_FRONT_PADDING, QUEUE_REAR_PADDING);
      scheduleQueueRunTimeByIndex = (index) -> scheduleQueue.get(index).getRunTime();
    }
    
    public void addTask(TaskWrapper task) {
      if(task.initalRunDelay() <=0 ) {
        addExecute(task);
      } else {
        addScheduled(task);
      }
    }
    
    public boolean isEmpty() {
      return executeQueue.isEmpty() && scheduleQueue.isEmpty();
    }
    
    public long timeSinceLastPull() {
      return Clock.accurateForwardProgressingMillis() - lastPulled;
    }
    
    /**
     * Adds a task for immediate execution.  No safety checks are done at this point, the task 
     * will be immediately added and available for consumption.
     * 
     * @param task Task to add to end of execute queue
     */
    public void addExecute(TaskWrapper task) {
      executeQueue.add(task);
      
      queueListener.handleQueueUpdate();
    }
    
    /**
     * Adds a task for delayed execution.  No safety checks are done at this point.  This call 
     * will safely find the insertion point in the scheduled queue and insert it into that 
     * queue.
     * 
     * @param task Task to insert into the schedule queue
     */
    public void addScheduled(TaskWrapper task) {
      int insertionIndex;
      synchronized (scheduleQueue.getModificationLock()) {
        insertionIndex = SortUtils.getInsertionEndIndex(scheduleQueueRunTimeByIndex, 
                                                        scheduleQueue.size() - 1, 
                                                        task.getRunTime(), true);
        scheduleQueue.add(insertionIndex, task);
      }
      
      if (insertionIndex == 0) {
        queueListener.handleQueueUpdate();
      }
    }
    
    /**
     * Removes a given callable from the internal queues (if it exists).
     * 
     * @param task Callable to search for and remove
     * @return {@code true} if the task was found and removed
     */
    public boolean remove(Callable<?> task) {
      {
        Iterator<? extends TaskWrapper> it = executeQueue.iterator();
        while (it.hasNext()) {
          TaskWrapper tw = it.next();
          if (ContainerHelper.isContained(tw.getContainedRunnable(), task) && executeQueue.remove(tw)) {
            tw.invalidate();
            return true;
          }
        }
      }
      synchronized (scheduleQueue.getModificationLock()) {
        Iterator<? extends TaskWrapper> it = scheduleQueue.iterator();
        while (it.hasNext()) {
          TaskWrapper tw = it.next();
          if (ContainerHelper.isContained(tw.getContainedRunnable(), task)) {
            tw.invalidate();
            it.remove();
            
            return true;
          }
        }
      }
      
      return false;
    }
    
    /**
     * Removes a given Runnable from the internal queues (if it exists).
     * 
     * @param task Runnable to search for and remove
     * @return {@code true} if the task was found and removed
     */
    public boolean remove(Runnable task) {
      {
        Iterator<? extends TaskWrapper> it = executeQueue.iterator();
        while (it.hasNext()) {
          TaskWrapper tw = it.next();
          if (ContainerHelper.isContained(tw.getContainedRunnable(), task) && executeQueue.remove(tw)) {
            tw.invalidate();
            return true;
          }
        }
      }
      synchronized (scheduleQueue.getModificationLock()) {
        Iterator<? extends TaskWrapper> it = scheduleQueue.iterator();
        while (it.hasNext()) {
          TaskWrapper tw = it.next();
          if (ContainerHelper.isContained(tw.getContainedRunnable(), task)) {
            tw.invalidate();
            it.remove();
            
            return true;
          }
        }
      }
      
      return false;
    }
    
    /**
     * Call to get the total quantity of tasks within both stored queues.  This returns the total 
     * quantity of items in both the execute and scheduled queue.  If there are scheduled tasks 
     * which are NOT ready to run, they will still be included in this total.
     * 
     * @return Total quantity of tasks queued
     */
    public int queueSize() {
      return executeQueue.size() + scheduleQueue.size();
    }
    
    public void drainQueueInto(List<TaskWrapper> removedTasks) {
      clearQueue(executeQueue, removedTasks);
      synchronized (scheduleQueue.getModificationLock()) {
        clearQueue(scheduleQueue, removedTasks);
      }
    }
    
    private static void clearQueue(Collection<? extends TaskWrapper> queue, 
                                   List<TaskWrapper> resultList) {
      boolean resultWasEmpty = resultList.isEmpty();
//      Iterator<? extends TaskWrapper> it = queue.iterator();
//      
//      while (it.hasNext()) {
//        TaskWrapper tw = it.next();
//        // no need to cancel and return tasks which are already canceled
//        if (! (tw.task instanceof Future) || ! ((Future<?>)tw.task).isCancelled()) {
//          tw.invalidate();
//          // don't return tasks which were used only for internal behavior management
//          if (! (tw.task instanceof InternalRunnable)) {
//            if (resultWasEmpty) {
//              resultList.add(tw);
//            } else {
//              resultList.add(SortUtils.getInsertionEndIndex((index) -> 
//              resultList.get(index).getRunTime(), 
//              resultList.size() - 1, 
//              tw.getRunTime(), true), 
//                             tw);
//            }
//          }
//        }
//      }
      queue.clear();
    }
    
    /**
     * Gets the next task from this {@link QueueSet}.  This inspects both the execute queue and 
     * against scheduled tasks to determine which task in this {@link QueueSet} should be executed 
     * next.
     * <p>
     * The task returned from this may not be ready to executed, but at the time of calling it 
     * will be the next one to execute.
     * 
     * @return TaskWrapper which will be executed next, or {@code null} if there are no tasks
     */
    public TaskWrapper getNextTask() {
      TaskWrapper schTask = null;
      if(!scheduleQueue.isEmpty() && !checkingScheduled.get() && checkingScheduled.compareAndSet(false, true)) {
        while((schTask = scheduleQueue.peekFirst()) != null && schTask.getScheduleDelay() <= 0) {
          executeQueue.add(scheduleQueue.poll());
        }
        checkingScheduled.set(false);
      }
      TaskWrapper execTask = executeQueue.poll();
      if(execTask != null) {
        return execTask;
      }
      return schTask;
    }
  }
  
  /**
   * A service which manages the execute queues.  It runs a task to consume from the queues and 
   * execute those tasks as workers become available.  It also manages the queues as tasks are 
   * added, removed, or rescheduled.
   * 
   * @since 3.4.0
   */
  protected static class QueueManager {
    protected final QueueSet highPriorityQueueSet;
    protected final QueueSet lowPriorityQueueSet;
    protected final QueueSet starvablePriorityQueueSet;
    private volatile long maxWaitForLowPriorityInMs;
    
    public QueueManager(QueueSetListener queueSetListener, long maxWaitForLowPriorityInMs) {
      this.highPriorityQueueSet = new QueueSet(queueSetListener);
      this.lowPriorityQueueSet = new QueueSet(queueSetListener);
      this.starvablePriorityQueueSet = new QueueSet(queueSetListener);
      
      // call to verify and set values
      setMaxWaitForLowPriority(maxWaitForLowPriorityInMs);
    }
    
    public void addTask(TaskWrapper task) {
      switch(task.getPriority()) {
        case High:
          highPriorityQueueSet.addTask(task);
          return;
        case Low:
          highPriorityQueueSet.addTask(task);
          return;
        case Starvable:
          highPriorityQueueSet.addTask(task);
          return;
      }
    }
    
    /**
     * Returns the {@link QueueSet} for a specified priority.
     * 
     * @param priority Priority that should match to the given {@link QueueSet}
     * @return {@link QueueSet} which matches to the priority
     */
    public QueueSet getQueueSet(TaskPriority priority) {
      if (priority == TaskPriority.High) {
        return highPriorityQueueSet;
      } else if (priority == TaskPriority.Low) {
        return lowPriorityQueueSet;
      } else {
        return starvablePriorityQueueSet;
      }
    }
    
    /**
     * Removes any tasks waiting to be run.  Will not interrupt any tasks currently running.  But 
     * will avoid additional tasks from being run (unless they are allowed to be added during or 
     * after this call).  
     * <p>
     * If tasks are added concurrently during this invocation they may or may not be removed.
     * 
     * @return List of runnables which were waiting in the task queue to be executed (and were now removed)
     */
    public List<Runnable> clearQueue() {
      List<TaskWrapper> wrapperList = new ArrayList<>(highPriorityQueueSet.queueSize() + 
          lowPriorityQueueSet.queueSize() + 
          starvablePriorityQueueSet.queueSize());
      highPriorityQueueSet.drainQueueInto(wrapperList);
      lowPriorityQueueSet.drainQueueInto(wrapperList);
      starvablePriorityQueueSet.drainQueueInto(wrapperList);
      
      return ContainerHelper.getContainedRunnables(wrapperList);
    }
    
    /**
     * Gets the next task currently queued for execution.  This task may be ready to execute, or 
     * just queued.  If a queue update comes in, this must be re-invoked to see what task is now 
     * next.  If there are no tasks ready to be executed this will simply return {@code null}.
     * 
     * @return Task to be executed next, or {@code null} if no tasks at all are queued
     */
    public TaskWrapper getNextTask() {
      // First compare between high and low priority task queues
      // then depending on that state, we may check starvable
      TaskWrapper nextTask = null;
      if (lowPriorityQueueSet.isEmpty() && !highPriorityQueueSet.isEmpty()) {
        nextTask = highPriorityQueueSet.getNextTask();
      } else if (!lowPriorityQueueSet.isEmpty() && !highPriorityQueueSet.isEmpty()) { 
          if(highPriorityQueueSet.timeSinceLastPull() > maxWaitForLowPriorityInMs) {
            nextTask = lowPriorityQueueSet.getNextTask();
          } else {
            nextTask = highPriorityQueueSet.getNextTask();
          }
      } else if (!lowPriorityQueueSet.isEmpty() && highPriorityQueueSet.isEmpty()) {
        nextTask =  lowPriorityQueueSet.getNextTask();
      } else {
        nextTask = starvablePriorityQueueSet.getNextTask();
      }
      return nextTask;
    }
    
    /**
     * Removes the runnable task from the execution queue.  It is possible for the runnable to 
     * still run until this call has returned.
     * <p>
     * Note that this call has high guarantees on the ability to remove the task (as in a complete 
     * guarantee).  But while this is being invoked, it will reduce the throughput of execution, 
     * so should NOT be used extremely frequently.
     * 
     * @param task The original runnable provided to the executor
     * @return {@code true} if the runnable was found and removed
     */
    public boolean remove(Runnable task) {
      return highPriorityQueueSet.remove(task) || lowPriorityQueueSet.remove(task) || 
          starvablePriorityQueueSet.remove(task);
    }
    
    /**
     * Removes the callable task from the execution queue.  It is possible for the callable to 
     * still run until this call has returned.
     * <p>
     * Note that this call has high guarantees on the ability to remove the task (as in a complete 
     * guarantee).  But while this is being invoked, it will reduce the throughput of execution, 
     * so should NOT be used extremely frequently.
     * 
     * @param task The original callable provided to the executor
     * @return {@code true} if the callable was found and removed
     */
    public boolean remove(Callable<?> task) {
      return highPriorityQueueSet.remove(task) || lowPriorityQueueSet.remove(task) || 
          starvablePriorityQueueSet.remove(task);
    }
    
    /**
     * Changes the max wait time for low priority tasks.  This is the amount of time that a low 
     * priority task will wait if there are ready to execute high priority tasks.  After a low 
     * priority task has waited this amount of time, it will be executed fairly with high priority 
     * tasks (meaning it will only execute the high priority task if it has been waiting longer than 
     * the low priority task).
     * 
     * @param maxWaitForLowPriorityInMs new wait time in milliseconds for low priority tasks during thread contention
     */
    public void setMaxWaitForLowPriority(long maxWaitForLowPriorityInMs) {
      ArgumentVerifier.assertNotNegative(maxWaitForLowPriorityInMs, "maxWaitForLowPriorityInMs");
      
      this.maxWaitForLowPriorityInMs = maxWaitForLowPriorityInMs;
    }
    
    /**
     * Getter for the amount of time a low priority task will wait during thread contention before 
     * it is eligible for execution.
     * 
     * @return currently set max wait for low priority task
     */
    public long getMaxWaitForLowPriority() {
      return maxWaitForLowPriorityInMs;
    }
  }
  
  /**
   * Small interface so we can determine internal tasks which were not submitted by users.  That 
   * way they can be filtered out (for example in draining the queue).
   * 
   * @since 4.3.0
   */
  protected interface InternalRunnable extends Runnable {
    // nothing added here
  }
}
