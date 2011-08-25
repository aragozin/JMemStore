/**
 *  Copyright 2011 Alexey Ragozin
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.gridkit.offheap.jmemstore;

import org.gridkit.offheap.jmemstore.BinHash;
import org.junit.Test;

import junit.framework.Assert;

/**
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public class SplitHashTest {

	@Test
	public void test1() {
		Assert.assertEquals(511, BinHash.splitHash(-1, 512));
	}

	@Test
	public void test2() {
		Assert.assertEquals(510, BinHash.splitHash(-2, 512));
	}

	@Test
	public void test3() {
		Assert.assertEquals(0, BinHash.splitHash(0, 512));
	}

	@Test
	public void test4() {
		Assert.assertEquals(1, BinHash.splitHash(1, 512));
	}

	@Test
	public void test5() {
		Assert.assertEquals(511, BinHash.splitHash(511, 512));
	}

	@Test
	public void test6() {
		Assert.assertEquals(255, BinHash.splitHash(511, 511));
	}

	@Test
	public void test7() {
		Assert.assertEquals(256, BinHash.splitHash(256, 511));
	}

	@Test
	public void test8() {
		Assert.assertEquals(256, BinHash.splitHash(-256, 511));
	}
	
	@Test
	public void sizeAllignTest() {
		int segmentCount = 4;
		int capacity = 0;
		while(capacity < 100) {
			// assumed newCap = capacity + 1
			
			int slot = capacity % segmentCount;
			int oldSegSize = alignSegmentSize(capacity / segmentCount);
			int newSegSize = alignSegmentSize(1 + (capacity / segmentCount));
			if (oldSegSize != newSegSize) {
				System.out.println("Resize slot " + slot + ": " + oldSegSize + " -> " + newSegSize);
				
			}
			++capacity;
			System.out.println("Slot " + slot + ", size=" + getSegmentSize(slot, capacity, segmentCount));
		}
	}
	
	private int getSegmentSize(int n, int capacity, int segmentCount) {
		int size = (capacity / segmentCount + (n < (capacity % segmentCount) ? 1 : 0));
		return alignSegmentSize(size);
	}
	
	private int alignSegmentSize(int cap) {
		if (cap == 0) {
			return 0;
		}
		++cap;
		int allignment = 0xF;
		if (cap > 256) {
			allignment = 0x3F;
		}
		else if (cap > 1024) {
			allignment = 0xFF;
		}
		else if (cap > 4096) {
			allignment = 0x3FF;
		}
		cap = (cap + allignment) & (~allignment);
		--cap;
		return cap;
	}
	
//	@Test
//	public void capacity() {
//		int capacity = 305530;
//		
//    	int nRound = Integer.highestOneBit(capacity);
//    	int nSplit = (capacity) & ~nRound;
//    	int nLast = capacity;
//
//    	System.out.println("nRound: " + nRound);
//    	System.out.println("nSplit: " + nSplit);
//    	System.out.println("nLast: " + nLast);
//		
//	}
}
