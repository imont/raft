package io.tetrapod.raft;

import org.junit.*;

import static org.junit.Assert.assertEquals;

public class RaftUtilTest {

   @Test
   public void testLongs1() {
      long val = 123456789101112L;
      byte[] b = RaftUtil.toBytes(val);
      long val2 = RaftUtil.toLong(b);
      assertEquals(val, val2);
   }

   @Test
   public void testLongs2() {
      long val = -123456789101112L;
      byte[] b = RaftUtil.toBytes(val);
      long val2 = RaftUtil.toLong(b);
      assertEquals(val, val2);
   }

}
