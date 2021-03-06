package org.threadly.concurrent.lock;

import static org.junit.Assert.*;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class StripedLockTest {
  private static final int LOCK_QTY = 10;
  
  private StripedLock sLock = new StripedLock(LOCK_QTY);
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorNegativeConcurrencyFail() {
    new StripedLock(-10);
    
    fail("Exception should have been thrown");
  }
  
  @Test
  public void getLockObjectTest() {
    Object testKey1 = new Object();
    Object testKey2 = new Object();
    while (testKey2.hashCode() % LOCK_QTY == testKey1.hashCode() % LOCK_QTY) {
      testKey2 = new Object();  // verify they will get different locks
    }
    
    Object lock1 = sLock.getLock(testKey1);
    assertNotNull(lock1);
    Object lock2 = sLock.getLock(testKey2);
    
    assertTrue(lock1 != lock2);
    assertTrue(sLock.getLock(testKey1) == lock1);
    assertTrue(sLock.getLock(testKey2) == lock2);
  }
  
  @Test
  public void getLockNullTest() {
    assertTrue(sLock.getLock(0) == sLock.getLock(null));
  }
  
  @Test
  public void getLockHashCodeTest() {
    Object testKey1 = new Object();
    
    Object lock = sLock.getLock(testKey1.hashCode());
    assertNotNull(lock);
    
    assertTrue(sLock.getLock(testKey1) == lock);
    assertTrue(sLock.getLock(testKey1.hashCode() + LOCK_QTY) == lock);
  }
}
