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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
class InHeapMemoryStoreBackend implements MemoryStoreBackend {

	private final static int PAGE_HEADER = 32; // leaves 0 and 1 pointers as special values
	private final static int ALIGNMENT = 4; // 16 bytes
	
	private final int pageSize;
	private final int pageUsageLimit;
	private final int offsetMask;
	private final int pageShift;
	
	private AtomicLong memUsed = new AtomicLong();
	private long memUsageLimit;
	
	private ReentrantLock[] allocationLock;
	private AtomicInteger pagesInUse = new AtomicInteger();
	private AtomicInteger[] top;
	private AtomicReferenceArray<ByteChunk> pages;
	private AtomicIntegerArray pageUtilizations;
	private AtomicInteger evacuationQueueLength = new AtomicInteger(); 
	private int evacuationQueueLimit; 
	private long[] pageTimestamps;
	private volatile boolean[] pagesForCleanUp;
	
	private ReentrantLock evacuationLock = new ReentrantLock();
	private int evacuationPage;
	private int evacuationPointer;	
	private AtomicInteger pageReleaseCounter = new AtomicInteger();
	
	private float scavengeGcThreshold = 0.8f;
	private float minGcThreshold = 0.4f;
	private float gcThreshold = minGcThreshold;
	
	private volatile int fence;
	
	public InHeapMemoryStoreBackend(int pageSize, int pageUsageLimit, int allocNumber) {		
		this.pageSize = pageSize;
		if (pageSize != Integer.highestOneBit(pageSize) || pageSize > 1 << 30) {			
			throw new IllegalArgumentException("Invalid page size " + pageSize + ", valid page size should be power of 2 and no more than 1Gb");
		}
		this.offsetMask = (pageSize - 1) >> ALIGNMENT;
		this.pageShift = Integer.bitCount(offsetMask);
		if (1l * pageSize * pageUsageLimit > 32l << 30) {
			throw new IllegalArgumentException("Single manager cannot handle more than 32Gb of memory");
		}
		
		this.pageUsageLimit = pageUsageLimit;
		this.memUsageLimit = ((long)pageSize) * pageUsageLimit;
		
		this.memUsed.set(0);
		this.pagesInUse.set(0);
		
		this.pages = new AtomicReferenceArray<ByteChunk>(pageUsageLimit);
		this.pageUtilizations = new AtomicIntegerArray(pageUsageLimit);
		this.pageTimestamps = new long[pageUsageLimit];
		this.pagesForCleanUp = new boolean[pageUsageLimit];
		
//		evacuationQueueLimit = pageUsageLimit / 16;
//		evacuationQueueLimit = evacuationQueueLimit < 2 ? 2 : evacuationQueueLimit;
		evacuationQueueLimit = pageUsageLimit;
		
		// allocate first page
		allocationLock = new ReentrantLock[allocNumber];
		top = new AtomicInteger[allocNumber];
		for(int i = 0; i!= allocNumber; ++i) {
			allocationLock[i] = new ReentrantLock(); 
			top[i] = new AtomicInteger();
			top[i].set(pointer(newPage(i), PAGE_HEADER));
		}
		
//		dumpStatistics();
	}
	
	public int page(int pointer) {
		int page = (0x7FFFFFFF & pointer) >> pageShift;
		return page;
	}

	public int offset(int pointer) {
		int offs = (offsetMask & pointer) << ALIGNMENT;
//		if (offs >= pageSize) {
//			// debug;
//			new String();
//		}
		return offs;
	}
	
	int size(int sv) {
		return 0x7FFFFFFF & sv;
	}
	
	boolean erased(int sv) {
		return (0x80000000 & sv) != 0;
	}
	
	int pointer(int page, int offset) {
		int pointer = offsetMask & (offset >> ALIGNMENT);
//		if (pointer == 0 && offset != 0) {
//			throw new AssertionError();
//		}
		pointer |= (0x7FFFFFFF & (page << pageShift));
//		if (page != page(pointer)) {
//			new String();
//		}
//		if (offset != offset(pointer)) {
//			new String();
//		}
		return pointer;
	}
	
	/* (non-Javadoc)
	 * @see org.gridkit.coherence.offheap.storage.memlog.MemoryStoreBackend#get(int)
	 */
	public ByteChunk get(int pointer) {
		validate(pointer);
		int page = page(pointer);
		int offs = offset(pointer);
		if (offs < PAGE_HEADER) {
			throw new IllegalArgumentException("Invalid pointer " + Long.toHexString(pointer));
		}
		
		ByteChunk chunk = pages.get(page);
		if (chunk == null) {
			throw new IllegalArgumentException("Broken pointer " + Long.toHexString(pointer) + " page " + Integer.toHexString(page) + " is not allocated");
		}
		int len = size(chunk.intAt(offs));
		if (offs + 4 + len > pageSize) {
			new String();
		}
		return chunk.subChunk(offs + 4, len - 4);
	}	
	
	@Override
	public void update(int pointer, ByteChunk bytes) {
		// TODO 
		// no need to update
	}
	
	public int readInt(int pointer, int offset) {
		validate(pointer);
		int page = page(pointer);
		int offs = offset(pointer);
		if (offs < PAGE_HEADER) {
			throw new IllegalArgumentException("Invalid pointer " + Long.toHexString(pointer));
		}
		
		ByteChunk chunk = pages.get(page);
		if (chunk == null) {
			throw new IllegalArgumentException("Broken pointer " + Long.toHexString(pointer) + " page " + Integer.toHexString(page) + " is not allocated");
		}
		int len = size(chunk.intAt(offs));
		if (offs + 4 + len > pageSize) {
			new String();
		}
		if (offset + 4 > len) {
			throw new IndexOutOfBoundsException("Requested offset " + offset + ", chunk lenght " + len);
		}
		
		return chunk.intAt(offs + 4 + offset);
	}

	public void writeInt(int pointer, int offset, int value) {
		validate(pointer);
		int page = page(pointer);
		int offs = offset(pointer);
		if (offs < PAGE_HEADER) {
			throw new IllegalArgumentException("Invalid pointer " + Long.toHexString(pointer));
		}
		
		ByteChunk chunk = pages.get(page);
		if (chunk == null) {
			throw new IllegalArgumentException("Broken pointer " + Long.toHexString(pointer) + " page " + Integer.toHexString(page) + " is not allocated");
		}
		int len = size(chunk.intAt(offs));
		if (offs + 4 + len > pageSize) {
			new String();
		}
		if (offset + 4 > len) {
			throw new IndexOutOfBoundsException("Requested offset " + offset + ", chunk lenght " + len);
		}
		
		chunk.putInt(offs + 4 + offset, value);
	}

	/* (non-Javadoc)
	 * @see org.gridkit.coherence.offheap.storage.memlog.MemoryStoreBackend#allocate(int, int)
	 */
	public int allocate(int size, int allocNo) {
		if (size > pageSize >> 2) {
			// TODO allocate large objects in heap
			throw new IllegalArgumentException("Size is too large");
		}
		while(true) {
			int len = size;
			len += 4;
			int alen = align(len);
			
			int pp;
			while(true) {
				pp = top[allocNo].get();
				int offs;
				offs = offset(pp);
				// this is a dirty way to avoid perfect page fit edge case
				if (offs + alen + 1> pageSize) {
					allocationLock[allocNo].lock();
					try {
						pp = top[allocNo].get();
						offs = offset(pp);
						if (offs + alen + 1> pageSize) {
							int page = page(pp);
							int newPage = newPage(page + 1);
							System.out.println("Page allocated " + newPage);
							top[allocNo].set(pointer(newPage,PAGE_HEADER));
							if (pageUtilizations.get(page) == 0) {
								ByteChunk oldPage = pages.getAndSet(page, null);
								if (oldPage != null) {
									pagesInUse.decrementAndGet();
									pageReleaseCounter.incrementAndGet();
								}
							}
						}
						else {
							continue;
						}						
					}
					finally {
						allocationLock[allocNo].unlock();
					}
				}
				else {
					int npp = pointer(page(pp), offs + alen);
					if (top[allocNo].compareAndSet(pp, npp)) {
						break;
					}
				}
			}
			
			int page = page(pp);
			int offs = offset(pp);
			ByteChunk pageBuf = pages.get(page);
			// TODO debug
//			if (pageBuf == null) {
//				new String();
//			}
			if (pageBuf.intAt(offs) != 0) {
				int xx = top[allocNo].get();
				System.err.println("Dirty memory allocated!!!");
				System.err.println("Allocation pointer " + page(xx) + ":" + offset(xx) + " stream " + allocNo);
				dumpPage(page, pageBuf);
				throw new AssertionError("Allocation have failed (" + size + " requested). Dirty memory: " + page + ":" + offs);
			}
			pageBuf.putInt(offs, len);
			pageUtilizations.addAndGet(page, len);
			memUsed.addAndGet(len);			
			fence += 2;
			validate(pp);			
			return pp;
		}
	}

	private void dumpPage(int page, ByteChunk pageBuf) {
		System.err.println("Page dump, page " + page);
		int offs = PAGE_HEADER;
		while(offs < pageSize) {
			int size = pageBuf.intAt(offs);
			if (size == 0) {
				System.err.println("End of page 0x" + Integer.toHexString(offs) + "(" + offs + ")");
				break;
			}
			System.err.println("Chunk 0x" + Integer.toHexString(offs) + "(" + offs + ")" + ", size=" + size(size) + ", erased=" + erased(size));
			size = align(size(size));
			ByteChunk chunk = pageBuf.subChunk(offs, size);
			System.err.println(chunk.toString());
			offs += size;
		}
	}

	private int align(int len) {
		// TODO unhardcode alignment
		return (len + 0xF) & (0xFFFFFFF0);
	}
	
	private int newPage(int start) {
		ByteChunk chunk = new ByteChunk(new byte[pageSize]);
		pagesInUse.incrementAndGet();
		while(true) {
			for(int i = 0; i != pageUsageLimit; ++i) {
				int page = (start + i) % pageUsageLimit; 
				if (pages.get(page) == null) {
					pagesForCleanUp[page] = false;
					if (pages.compareAndSet(page, null, chunk)) {
						pageTimestamps[page] = System.nanoTime();
						return page;
					}
					else {
						continue;
					}
				}
			}
			// hit memory limit, should give scavenger some time to
			// recover pages
			System.out.println("Out of pages");
			LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
		}
	}

	/* (non-Javadoc)
	 * @see org.gridkit.coherence.offheap.storage.memlog.MemoryStoreBackend#release(int)
	 */
	public void release(int pointer) {
		int page = page(pointer);
		int offs = offset(pointer);
		if (offs < PAGE_HEADER) {
			throw new IllegalArgumentException("Invalid pointer " + Long.toHexString(pointer));
		}
		if (page >= pages.length()) {
			// TODO allocate large objects in heap
			throw new IllegalArgumentException("Invalid pointer " + Integer.toHexString(pointer));
		}

		
		ByteChunk pageBuf = pages.get(page);
		int len = pageBuf.intAt(offs);
		int newSize = pageUtilizations.addAndGet(page, -len);
		memUsed.addAndGet(-len);
		
		if (!pagesForCleanUp[page]) {
			checkPageUsage(page);
		}
		if (newSize == 0) {
			System.out.println("Page " + page + " has no more data");

			int allocNo = -1;
			for(int j = 0; j != top.length; ++j) {
				if (page(top[j].get()) == page) {
					allocNo = j;
					break;
				}
			}
			// TODO is race condition possible ?
			if (pageUtilizations.get(page) == 0 && allocNo == -1) {
				if (pages.compareAndSet(page, pageBuf, null)) {
					pagesInUse.decrementAndGet();
					pageReleaseCounter.incrementAndGet();
					if (pagesForCleanUp[page]) {
						evacuationQueueLength.decrementAndGet();
					}
				}
			}

			evacuationLock.lock();
			try {
				if (evacuationPage == page) {
					evacuationPage = -1;
					evacuationPointer = 0;
				}
			}
			finally {
				evacuationLock.unlock();
			}
		}
		
		// mark chunk as deleted
		pageBuf.putInt(offs, 0x80000000 | len);
	}

	private void checkPageUsage(int page) {
		int allocNo = -1;
		for(int j = 0; j != top.length; ++j) {
			if (page(top[j].get()) == page) {
				allocNo = j;
				break;
			}
		}
		if (allocNo == -1) {
			int usage = pageUtilizations.get(page);
			if (usage < (gcThreshold * pageSize)) {
				int ql = evacuationQueueLength.get(); 
				if (pages.get(page) != null && ql < evacuationQueueLimit) {
					if (evacuationQueueLength.compareAndSet(ql, ql + 1)) {
						pagesForCleanUp[page] = true;
					}
				}
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see org.gridkit.coherence.offheap.storage.memlog.MemoryStoreBackend#shouldEvacuate()
	 */
	public boolean shouldEvacuate() {
		evacuationLock.lock();
		try {
			if (evacuationPage >= 0) {
				return true;
			}
			else {
				evacuationPage = choosePageToEvacuate();
				if (evacuationPage != -1) {
					ByteChunk pageBuf = pages.get(evacuationPage);
					if (pageBuf != null) {
						evacuationPointer = nextChunk(pageBuf, pointer(evacuationPage, PAGE_HEADER), true);
					}
				}
				return evacuationPage >= 0;
			}
		}
		finally {
			evacuationLock.unlock();
		}		
	}
	
	/* (non-Javadoc)
	 * @see org.gridkit.coherence.offheap.storage.memlog.MemoryStoreBackend#collectHashesForEvacuation(int[], int)
	 */
	public int collectHashesForEvacuation(int[] hashes, int len) {
		len = len == 0 ? hashes.length : len;
		evacuationLock.lock();
		try {

			int i;
			for (i = 0; i != len; ++i) {
				
				if (evacuationPointer == 0) {
					evacuationPage = choosePageToEvacuate();
					if (evacuationPage != -1) {
						ByteChunk pageBuf = pages.get(evacuationPage);
						if (pageBuf != null) {
							evacuationPointer = nextChunk(pageBuf, pointer(evacuationPage, PAGE_HEADER), true);
						}
					}
				}
	
				if (evacuationPointer == 0) {
					break;
				}
				else {
					int page = page(evacuationPointer);
					int offset = offset(evacuationPointer);
					ByteChunk pageBuf = pages.get(page);
					if (pageBuf == null) {
						evacuationPointer = 0;
						break;
					}
					int hash = pageBuf.intAt(offset + 4);
					evacuationPointer = nextChunk(pageBuf, evacuationPointer, false);
					hashes[i] = hash;
//					if (evacuationPointer == 0) {
//						break;
//					}
				}
			}
			return i;
		}
		finally {
			evacuationLock.unlock();
		}
	}
	
	private int choosePageToEvacuate() {
		int page = -1;
		int minUsed = pageSize;
		for (int i = 0; i != pages.length(); ++i) {
			if (pageUtilizations.get(i) == 0) {
				continue;
			}
			if (evacuationPage == i) {
				continue;
			}
			boolean tp = false;
			for (int j = 0; j != top.length; ++j) {
				if (page(top[j].get()) == i) {
					tp = true;
					break;
				}
			}
			if (!tp) {
				int usage = pageUtilizations.get(i);
				if (minUsed > usage) {
					minUsed = usage;
					page = i;
				}
			}
		}
		
		if (minUsed < scavengeGcThreshold * pageSize) {
			System.out.println("Next evacuation page is " + page + " utilization " + ((float)minUsed) / pageSize);
			evacuationPage = page;
			return page;
		}
		
		return -1;
	}

	private int nextChunk(ByteChunk pageBuf, int pointer, boolean inclusive) {
		validate(pointer);
		
		int page = page(pointer);
		int offs = offset(pointer);
		
		pageBuf = pageBuf != null ? pageBuf : pages.get(page);
		
		if (inclusive && !erased(pageBuf.intAt(offs))) {
			return pointer;
		}
		
		int len = align(size(pageBuf.intAt(offs)));
		offs += len;
		
		while(offs < pageSize) {
			int sv = pageBuf.intAt(offs);
			if (sv == 0) {
				break;
			}
			else {
				if (erased(sv)) {
					offs += align(size(sv));
					continue;
				}
				else {
					return pointer(page, offs);
				}
			}
		}		
		return 0;
	}
	
	/* (non-Javadoc)
	 * @see org.gridkit.coherence.offheap.storage.memlog.MemoryStoreBackend#isMarkedForRecycle(int)
	 */
	public boolean isMarkedForRecycle(int pp) {
		int page = page(pp);
//		if ((page < 0) || (page > pagesForCleanUp.length)) {
//			// for debug
//			new String();
//		}
		return page == evacuationPage || pagesForCleanUp[page];
	}
	
	/* (non-Javadoc)
	 * @see org.gridkit.coherence.offheap.storage.memlog.MemoryStoreBackend#getMemUsage()
	 */
	public long getMemUsage() {
		return memUsed.get();
	}
	
	// for debug only
	void validate(int pp) {
		if (true) {
			return;
		}
		else {
			int page = page(pp);
			int offs = offset(pp);
			 
			if (offs < PAGE_HEADER) {
				throw new AssertionError();
			}
			
//			if (page == 0) {
//				return;
//			}
			 
			if (page <0 || page > pages.length()) {
				throw new AssertionError();
			}
			 
			ByteChunk buf = pages.get(page);
			if (buf == null) {
				throw new IllegalArgumentException("Pointer " + Integer.toHexString(pp) + " points to non existent page");
			}
			int roll = PAGE_HEADER;
			int oldRoll = 0;
			while(true) {
				// forcing memory fence
				synchronized(buf) {
					if (roll == offs) {
						int size = buf.intAt(roll);
						if (align(size) + offs > pageSize) {
							throw new AssertionError();
						}
						return;
					}
					if (roll >offs) {
						throw new AssertionError();
					}
					int size = 0;
					for (int i = 0; i != 50; ++i) {
						synchronized (buf) {
							size = size(buf.intAt(roll));
							if (size != 0) {
								break;
							}
							Thread.yield();
						}
					}
					if (size == 0) {						
						throw new AssertionError();
					}
					oldRoll = roll;
					roll += align(size);
					if (roll >offs) {
						throw new AssertionError();
					}
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.gridkit.coherence.offheap.storage.memlog.MemoryStoreBackend#dumpStatistics()
	 */
	public void dumpStatistics() {

		long unallocated = 0;
		for(int j = 0; j != top.length; ++j) {
			int tp = top[j].get();
			unallocated += pageSize - offset(tp);
		}

		StringBuilder buf = new StringBuilder();
		buf.append("Pages allocated: ").append(pagesInUse.get()).append('/').append(pageUsageLimit).append(" (").append(pageSize).append(" per page)").append('\n');
		buf.append("Pages freed since last report: ").append(pageReleaseCounter.get()).append('\n');
		buf.append("Memory used: ").append(memUsed.get()).append('/').append(((long)pageUsageLimit) * pageSize).append('\n');
		buf.append("Page utilization: ").append(String.format("%f", ((double)memUsed.get() + unallocated) / (((double)pagesInUse.get()) * pageSize))).append('\n');
	
		for(int i = 0; i != pageUtilizations.length(); ++i) {
			int us = pageSize;
			for(int j = 0; j != top.length; ++j) {
				int tp = top[j].get();
				if (i == page(tp)) {
					us = offset(tp);
					break;
				}
			}
			
			int pu = pageUtilizations.get(i);
			if (pu > 0) {
				buf.append(i).append(" -> ").append(String.format("%f", ((double)pu) / (((double)us))));
				if (us < pageSize) {
					buf.append('*');
				}
				if (i == evacuationPage) {
					buf.append('!');
				}
				buf.append('\n');
			}
		}
		buf.append('\n');
		
		pageReleaseCounter.set(0);
		System.out.println(buf.toString());
	}
}
