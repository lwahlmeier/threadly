package org.threadly.concurrent.collections;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;

public class ArrayOfLinkedQueues<T> implements Queue<T>{
  
  private final ConcurrentLinkedQueue<T>[] queues;
  
  public ArrayOfLinkedQueues(int load) {
    queues = new ConcurrentLinkedQueue[load];
    for(int i=0; i<load; i++) {
      queues[i] = new ConcurrentLinkedQueue<T>();
    }
  }
  
  private int getIdByHash(Object o) {
//    System.out.println(o.hashCode()+":"+queues.length+":"+(o.hashCode()%queues.length));
    return o.hashCode()%queues.length;
  }

  @Override
  public boolean addAll(Collection<? extends T> arg0) {
    for(T t: arg0) {
      add(t);
    }
    return true;
  }

  @Override
  public void clear() {
    for(ConcurrentLinkedQueue<T> q: queues) {
      q.clear();
    }
  }

  @Override
  public boolean contains(Object arg0) {
    return queues[getIdByHash(arg0)].contains(arg0);
  }

  @Override
  public boolean containsAll(Collection<?> arg0) {
    for(Object o: arg0) {
      if(!queues[getIdByHash(o)].contains(o)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isEmpty() {
    for(ConcurrentLinkedQueue<T> q: queues) {
      if(!q.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Iterator<T> iterator() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean remove(Object arg0) {
    return queues[getIdByHash(arg0)].remove(arg0);
  }

  @Override
  public boolean removeAll(Collection<?> arg0) {
    boolean modified = false;
    for(Object o: arg0) {
      if(queues[getIdByHash(o)].remove(o)) {
        modified = true;
      }
    }
    return modified;
  }

  @Override
  public boolean retainAll(Collection<?> arg0) {
    boolean modified = false;
//TODO:
    return modified;
  }

  @Override
  public int size() {
    int size = 0;
    for(ConcurrentLinkedQueue<T> q: queues) {
      size+=q.size();
    }
    return size;
  }

  @Override
  public Object[] toArray() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T[] toArray(T[] arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean add(T arg0) {
    return queues[Math.floorMod(ThreadLocalRandom.current().nextInt(), queues.length)].add(arg0);
  }

  @Override
  public T element() {
    for(ConcurrentLinkedQueue<T> q: queues) {
      if(q.element() != null) {
        return q.element();
      }
    }
    return null;
  }

  @Override
  public boolean offer(T arg0) {
    return queues[getIdByHash(arg0)].offer(arg0);
  }

  @Override
  public T peek() {
    for(ConcurrentLinkedQueue<T> q: queues) {
      if(q.peek() != null) {
        return q.peek();
      }
    }
    return null;
  }

  @Override
  public T poll() {
    int start = Math.floorMod(ThreadLocalRandom.current().nextInt(), queues.length);
    int checks = 0;
    T item = null;
    while(checks < queues.length) {
      item = queues[start].poll();
      if(item != null) {
        return item;
      }
      if(start == queues.length-1) {
        start=0;
      } else {
        start++;
      }
      checks++;
    }
    
    for(ConcurrentLinkedQueue<T> q: queues) {
      item = q.poll();
      if(item != null) {
        return item;
      }
    }
    return null;
  }

  @Override
  public T remove() {
    T item = poll();
    if(item == null) {
      throw new NoSuchElementException();
    }
    return item;
  }
  
}
