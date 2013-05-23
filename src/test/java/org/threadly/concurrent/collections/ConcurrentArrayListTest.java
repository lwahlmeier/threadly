package org.threadly.concurrent.collections;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.concurrent.lock.NativeLock;
import org.threadly.concurrent.lock.VirtualLock;

@SuppressWarnings("javadoc")
public class ConcurrentArrayListTest {
  private static final int TEST_QTY = 10;
  
  private ConcurrentArrayList<String> testList;
  
  @Before
  public void setup() {
    testList = new ConcurrentArrayList<String>();
  }
  
  @After
  public void tearDown() {
    testList = null;
  }
  
  @Test
  public void getModificationLockTest() {
    VirtualLock testLock = new NativeLock();
    ConcurrentArrayList<String> testList = new ConcurrentArrayList<String>(testLock);
    
    assertTrue(testLock == testList.getModificationLock());
  }
  
  @Test
  public void setFrontPaddingTest() {
    testList.setFrontPadding(1);
    assertEquals(testList.getFrontPadding(), 1);
    
    // make some modifications
    testList.add("foo");
    testList.add("bar");
    testList.remove(0);
    
    assertEquals(testList.getFrontPadding(), 1);
  }

  @Test (expected = IllegalArgumentException.class)
  public void setFrontPaddingFail() {
    testList.setFrontPadding(-1);
  }
  
  @Test
  public void setRearPaddingTest() {
    testList.setRearPadding(1);
    assertEquals(testList.getRearPadding(), 1);
    
    // make some modifications
    testList.add("foo");
    testList.add("bar");
    testList.remove(0);
    
    assertEquals(testList.getRearPadding(), 1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setRearPaddingFail() {
    testList.setRearPadding(-1);
  }

  @Test
  public void sizeTest() {
    ListTests.sizeTest(testList);
  }

  @Test
  public void isEmptyTest() {
    ListTests.isEmptyTest(testList);
  }
  
  @Test
  public void getTest() {
    ListTests.getTest(testList);
  }
  
  @Test
  public void indexOfTest() {
    ListTests.indexOfTest(testList);
  }
  
  @Test
  public void lastIndexOfTest() {
    ListTests.lastIndexOfTest(testList);
  }
  
  @Test
  public void containsTest() {
    ListTests.containsTest(testList);
  }
  
  @Test
  public void containsAllTest() {
    ListTests.containsAllTest(testList);
  }
  
  @Test
  public void toArrayTest() {
    ListTests.toArrayTest(testList);
  }
  
  @Test
  public void clearTest() {
    ListTests.clearTest(testList);
    
    assertNull(testList.peek());
  }
  
  @Test
  public void addFirstTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.addFirst(str);
      assertEquals(testList.getFirst(), str);
    }
  }
  
  @Test
  public void addLastTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.addLast(str);
      assertEquals(testList.getLast(), str);
    }
  }
  
  @Test
  public void peekFirstTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.addFirst(str);
      assertEquals(testList.peekFirst(), str);
    }
  }
  
  @Test
  public void peekLastTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.addLast(str);
      assertEquals(testList.peekLast(), str);
    }
  }
  
  @Test
  public void removeAllTest() {
    ListTests.removeAllTest(testList);
  }
  
  @Test
  public void removeFirstOccurrenceTest() {
    List<String> firstStr = new ArrayList<String>(TEST_QTY);
    List<String> secondStr = new ArrayList<String>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str1 = Integer.toString(i);
      firstStr.add(str1);
      String str2 = Integer.toString(i);
      secondStr.add(str2);
      testList.add(str1);
      testList.add(str2);
    }
    
    
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.removeFirstOccurrence(str);
    }
    
    assertEquals(testList.size(), secondStr.size());
    
    Iterator<String> it = secondStr.iterator();
    Iterator<String> testIt = testList.iterator();
    while (it.hasNext()) {
      assertTrue(testIt.next() == it.next());
    }
  }
  
  @Test
  public void removeLastOccurrenceTest() {
    List<String> firstStr = new ArrayList<String>(TEST_QTY);
    List<String> secondStr = new ArrayList<String>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str1 = Integer.toString(i);
      firstStr.add(str1);
      String str2 = Integer.toString(i);
      secondStr.add(str2);
      testList.add(str1);
      testList.add(str2);
    }
    
    
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.removeLastOccurrence(str);
    }
    
    assertEquals(testList.size(), firstStr.size());
    
    Iterator<String> it = firstStr.iterator();
    Iterator<String> testIt = testList.iterator();
    while (it.hasNext()) {
      assertTrue(testIt.next() == it.next());
    }
  }
  
  @Test
  public void removeIndexTest() {
    ListTests.removeIndexTest(testList);
  }
  
  /*@Test
  public void repositionSearchForwardTest() {
    // TODO - implement
  }
  
  @Test
  public void repositionSearchBackwardTest() {
    // TODO - implement
  }
  
  @Test
  public void repositionIndexTest() {
    // TODO - implement
  }*/
  
  /* This also tests the ListIterator forwards, 
   * since this just defaults to that implementation
   */
  @Test
  public void testIterator() {
    ListTests.testIterator(testList);
  }
  
  @Test
  public void testListIteratorBackwards() {
    List<String> comparisionList = new ArrayList<String>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      comparisionList.add(str);
      testList.add(str);
    }
    
    ListIterator<String> clIt = comparisionList.listIterator(TEST_QTY);
    ListIterator<String> testIt = testList.listIterator(TEST_QTY);
    while (clIt.hasPrevious()) {
      assertTrue(testIt.hasPrevious());
      assertEquals(clIt.previous(), testIt.previous());
    }
  }
  
  @Test
  public void descendingIteratorTest() {
    Deque<String> comparisionDeque = new LinkedList<String>();
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      comparisionDeque.addLast(str);
      testList.add(str);
    }
    
    Iterator<String> clIt = comparisionDeque.descendingIterator();
    Iterator<String> testIt = testList.descendingIterator();
    while (clIt.hasNext()) {
      assertTrue(testIt.hasNext());
      assertEquals(clIt.next(), testIt.next());
    }
  }
}