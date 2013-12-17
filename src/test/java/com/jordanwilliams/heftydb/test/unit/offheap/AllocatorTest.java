package com.jordanwilliams.heftydb.test.unit.offheap;


import org.junit.Assert;
import org.junit.Test;

public class AllocatorTest {

    @Test
    public void allocateFreeTest(){
        long address = Allocator.allocate(128);
        Assert.assertTrue("Address is non-zero", address != 0);
        Allocator.free(address);
    }
}
