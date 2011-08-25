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

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
class OffHeapMemoryStoreBackend implements MemoryStoreBackend {

	final static int PAGE_HEADER = 32; // leaves 0 and 1 pointers as special values
	private final static int ALIGNMENT = 4; // 16 bytes, IMPORTANT if allignment constant is changing align() method should be updated
	
	private final static int DIRECT_BUFFER_SIZE = 64 << 20; // 64MiB
//	private final static int DIRECT_BUFFER_SIZE = 64 << 10; // 64KiB, for testing only
	
	final int pageSize;
	private final int pageUsageLimit;
	private final int offsetMask;
	private final int pageShift;
	
	private AtomicLong memUsed = new AtomicLong();
	private long memUsageLimit;
	
	private AtomicInteger pagesInUse = new AtomicInteger();
	private Allocator[] allocators;
	AtomicReferenceArray<Page> pages;
	
	private ReentrantLock evacuationLock = new ReentrantLock();
	private int evacuationPage;
	private int evacuationPointer;	
	private AtomicInteger pageReleaseCounter = new AtomicInteger();
	
	private float scavengeGcThreshold = 0.8f;
	private float minGcThreshold = 0.2f;
	private float gcThreshold = minGcThreshold;
	
	private volatile int fence;
	
	private OffHeapPageAllocator pageAllocator;
	
	public OffHeapMemoryStoreBackend(int pageSize, int pageUsageLimit, int allocNumber) {		
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
		
		this.pageAllocator = new OffHeapPageAllocator(pageSize, pageUsageLimit);
		
		this.memUsed.set(0);
		this.pagesInUse.set(0);
		
		this.pages = new AtomicReferenceArray<Page>(pageUsageLimit);
		
		// allocate first page
		allocators = new Allocator[allocNumber];
		for(int i = 0; i!= allocNumber; ++i) {
			allocators[i] = new Allocator("#" + i);
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
		
		Page chunk = pages.get(page);
		if (chunk == null) {
			throw new IllegalArgumentException("Broken pointer " + Long.toHexString(pointer) + " page " + Integer.toHexString(page) + " is not allocated");
		}
		int len = size(chunk.intAt(offs));
		if (offs + 4 + len > pageSize) {
			new String();
		}
		return chunk.subChunk(offs + 4, len - 4);
	}
	
	public int readInt(int pointer, int offset) {
		validate(pointer);
		int page = page(pointer);
		int offs = offset(pointer);
		if (offs < PAGE_HEADER) {
			throw new IllegalArgumentException("Invalid pointer " + page + ":" + offset + "[" + Long.toHexString(pointer) + "]");
		}
		
		Page chunk = pages.get(page);
		if (chunk == null) {
			throw new IllegalArgumentException("Broken pointer " + Long.toHexString(pointer) + " page " + page + " is not allocated");
		}
		int len = size(chunk.intAt(offs));
		if (offs + 4 + len > pageSize) {
			new String();
		}
		// 4 bytes of len is reserved for chunk size
		if (offset + 4 > len - 4) {
			throw new IndexOutOfBoundsException("Requested offset " + offset + ", chunk lenght " + (len - 4));
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
		
		Page chunk = pages.get(page);
		if (chunk == null) {
			throw new IllegalArgumentException("Broken pointer " + Long.toHexString(pointer) + " page " + Integer.toHexString(page) + " is not allocated");
		}
		int len = size(chunk.intAt(offs));
		if (offs + 4 + len > pageSize) {
			new String();
		}
		// 4 bytes of len is reserved for chunk size
		if (offset + 4 > len - 4) {
			throw new IndexOutOfBoundsException("Requested offset " + offset + ", chunk lenght " + len);
		}
		
		chunk.putInt(offs + 4 + offset, value);
	}

	@Override
	public void update(int pointer, ByteChunk bytes) {
		validate(pointer);
		int page = page(pointer);
		int offs = offset(pointer);
		if (offs < PAGE_HEADER) {
			throw new IllegalArgumentException("Invalid pointer " + Long.toHexString(pointer));
		}
		
		Page pageBuf = pages.get(page);
		if (pageBuf == null) {
			throw new IllegalArgumentException("Broken pointer " + Long.toHexString(pointer) + " page " + Integer.toHexString(page) + " is not allocated");
		}
		int len = size(pageBuf.intAt(offs));
		if (offs + 4 + len > pageSize) {
			new String();
		}
		
		if (bytes.lenght() != (len - 4)) {
			throw new IllegalArgumentException("Slot size does match buffer size. Slot:" + (len - 4) + ", buffer:" + bytes.lenght());
		}
		
		pageBuf.putBytes(offs + 4, bytes);
		
		// TODO debug
//		System.err.println("Memory updated " + page + ":" + offs);
//		System.err.println("Written: " + bytes);
//		dumpPage(pageBuf);
	}

	/* (non-Javadoc)
	 * @see org.gridkit.coherence.offheap.storage.memlog.MemoryStoreBackend#allocate(int, int)
	 */
	public int allocate(int size, int allocNo) {
		Allocator allocator = allocators[allocNo];
		return allocator.allocate(size);
	}
	
	private static int align(int len) {
		// TODO unhardcode alignment
		return (len + 0xF) & (0xFFFFFFF0);
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

		
		Page pageBuf = pages.get(page);
		int len = pageBuf.intAt(offs);
		pageBuf.updateMemUsage(-len);
		memUsed.addAndGet(-len);
		
		if (!pageBuf.isMarkedForEvacuation() && !pageBuf.isForAllocation()) {
			checkPageUsage(page);
		}
		
		if (pageBuf.getMemUsage() == 0 && !pageBuf.isForAllocation()) {
			System.out.println("Page " + page + " has no more data");

			releasePage(pageBuf);
		}
		
		if (pageBuf != null) {
			// mark chunk as deleted
			pageBuf.putInt(offs, 0x80000000 | len);
		}
	}

	private void checkPageUsage(int page) {
		Page pageBuf = pages.get(page);
		if (pageBuf != null) {
			if ( pageBuf.getMemUsage() < (gcThreshold * pageSize)) {
				pageBuf.markForEvacuation();
			}
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
						Page pageBuf = pages.get(evacuationPage);
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
					Page pageBuf = pages.get(page);
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
			Page pageBuf = pages.get(i);
			if (pageBuf == null || pageBuf.getMemUsage() == 0 || pageBuf.isForAllocation()) {
				continue;
			}
			if (evacuationPage == i) {
				continue;
			}
			int usage = pageBuf.getMemUsage();
			if (minUsed > usage) {
				minUsed = usage;
				page = i;
			}
		}
		
		if (minUsed < scavengeGcThreshold * pageSize) {
			System.out.println("Next evacuation page is " + page + " utilization " + ((float)minUsed) / pageSize);
			evacuationPage = page;
			Page pageBuf = pages.get(page);
			if (pageBuf != null) {
				pageBuf.markForEvacuation();
			}
			return page;
		}
		
		return -1;
	}

	private int nextChunk(Page pageBuf, int pointer, boolean inclusive) {
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
		Page pageBuf = pages.get(page);
//		if ((page < 0) || (page > pagesForCleanUp.length)) {
//			// for debug
//			new String();
//		}
		// pp may not be valid pointer at the moment of call
		return pageBuf != null && pageBuf.isMarkedForEvacuation();
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
			 
			Page buf = pages.get(page);
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
		for(int j = 0; j != allocators.length; ++j) {
			int tp = allocators[j].bumpPointer;
			unallocated += pageSize - offset(tp);
		}

		StringBuilder buf = new StringBuilder();
		buf.append("Pages allocated: ").append(pagesInUse.get()).append('/').append(pageUsageLimit).append(" (").append(pageSize).append(" per page)").append('\n');
		buf.append("Pages freed since last report: ").append(pageReleaseCounter.get()).append('\n');
		buf.append("Memory used: ").append(memUsed.get()).append('/').append(((long)pageUsageLimit) * pageSize).append('\n');
		buf.append("Page utilization: ").append(String.format("%f", ((double)memUsed.get() + unallocated) / (((double)pagesInUse.get()) * pageSize))).append('\n');
	
		for(int i = 0; i != pages.length(); ++i) {
			Page pageBuf = pages.get(i);
			if (pageBuf == null) {
				continue;
			}
			int us = pageSize;
			for(int j = 0; j != allocators.length; ++j) {
				int tp = allocators[j].bumpPointer;
				if (i == page(tp)) {
					us = offset(tp);
					break;
				}
			}
			
			int pu = pageBuf.memUsed;
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
	
	
	class OffHeapPageAllocator {
	
		final ByteBuffer[] buffers;
		final int pageSize;
		final int pageCount;
		final int pagesPerBuffer;
		final BitSet pages;
		final Semaphore allocPermits;
		final int directPageSize;
		
		public OffHeapPageAllocator(int pageSize, int pageCount) {
			this.directPageSize = DIRECT_BUFFER_SIZE > pageSize ? DIRECT_BUFFER_SIZE : pageSize;
			this.pageSize = pageSize;
			this.pageCount = pageCount;
			pagesPerBuffer = directPageSize / pageSize;
			if (directPageSize % pageSize != 0) {
				throw new AssertionError("Page size should be power of 2! (pageSize: " + pageSize + ")");
			}
			
			buffers = new ByteBuffer[(pageCount + pagesPerBuffer - 1) / pagesPerBuffer];
			pages = new BitSet(pageCount);
			allocPermits = new Semaphore(pageCount, true);
		}
		
		public Page allocate() {
			allocPermits.acquireUninterruptibly();
			return allocBuffer();
		}
		
		public Page tryAllocate() {
			if (allocPermits.tryAcquire()) {
				return allocBuffer();
			}
			else {
				return null;
			}
		}
		
		private synchronized Page allocBuffer() {
			int bufferId = pages.nextClearBit(0);
			if (bufferId >= pageCount) {
				throw new IllegalArgumentException("No more buffers");
			}
		
			pages.set(bufferId);
			
			ByteBuffer master = buffers[bufferId / pagesPerBuffer];
			if (master == null) {
				master = buffers[bufferId / pagesPerBuffer] = ByteBuffer.allocateDirect(directPageSize); 
			}
			
			int offset = pageSize * (bufferId % pagesPerBuffer);
			// working under critical section, not need to make a defensive copy of master buffer
			ByteBuffer bb = master;
			try {
				bb.position(0);
				bb.limit(bb.capacity());
				bb.position(offset);
				bb.limit(offset + pageSize);
				bb = bb.slice();
			}
			catch(IllegalArgumentException e) {
				e.printStackTrace();
				throw e;
			}
			
			// zeroing buffer
			for (int i = 0; i != pageSize; i += 8) {
				bb.putLong(i, 0);
			}
			
			return new Page(this, bb, bufferId);
		}
		
		public synchronized void release(int bufferId) {
			if (!pages.get(bufferId)) {
				// for Debug
				new String();
			}
			pages.clear(bufferId);
			allocPermits.release();
		}
	}
	
	private void releasePage(Page page) {
		if (!page.markForEvacuation()) {
			evacuationLock.lock();
			try {
				if (evacuationPage == page.getPageNo()) {
					evacuationPage = -1;
					evacuationPointer = 0;
				}
			}
			finally {
				evacuationLock.unlock();
			}
		}
		if (page.markForRelease()) {
			pages.compareAndSet(page.getPageNo(), page, null);
			pagesInUse.decrementAndGet();
			pageReleaseCounter.incrementAndGet();
			page.release();
		}
	}
	
	private static AtomicIntegerFieldUpdater<Allocator> ALLOC_BUMP_POINTER = AtomicIntegerFieldUpdater.newUpdater(Allocator.class, "bumpPointer");

	class Allocator {

		String name;
		ReentrantLock lock;
		volatile int bumpPointer;
		
		public Allocator(String name) {
			this.name = name;
			lock = new ReentrantLock();
			bumpPointer = pointer(newPage(0), PAGE_HEADER); 
		}
		
		public int allocate(int size) {
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
					pp = bumpPointer;
					int offs;
					offs = offset(pp);
					// this is a dirty way to avoid perfect page fit edge case
					if (offs + alen + 1> pageSize) {
						lock.lock();
						try {
							pp = bumpPointer;
							offs = offset(pp);
							if (offs + alen + 1> pageSize) {
								int page = page(pp);
								int newPage = newPage(page + 1);
								System.out.println("Page allocated " + newPage);
								bumpPointer = pointer(newPage,PAGE_HEADER);
								Page oldPage = pages.get(page);
								oldPage.markForStorage();
								if (oldPage.getMemUsage() == 0) {
									releasePage(oldPage);
								}
							}
							else {
								continue;
							}						
						}
						finally {
							lock.unlock();
						}
					}
					else {
						Page pageBuf = pages.get(page(pp));
						if (pageBuf == null) {
							continue;
						}
						
						// have to speculatively increment memory usage counter to prevent concurrent release of page
						pageBuf.updateMemUsage(len);
						int npp = pointer(page(pp), offs + alen);
						if (ALLOC_BUMP_POINTER.compareAndSet(this, pp, npp)) {
							break;
						}
						else {
							pageBuf.updateMemUsage(-len);
							if (!pageBuf.isForAllocation() && pageBuf.getMemUsage() == 0) {
								releasePage(pageBuf);
							}
						}
					}
				}
				
				int page = page(pp);
				int offs = offset(pp);
				Page pageBuf = pages.get(page);
				// TODO debug
//				if (pageBuf == null) {
//					new String();
//				}
				if (pageBuf.intAt(offs) != 0) {
					int xx = bumpPointer;
					System.err.println("Dirty memory allocated!!!");
					System.err.println("Allocation pointer " + page(xx) + ":" + offset(xx) + " stream " + this);
					dumpPage(pageBuf);
					throw new AssertionError("Allocation have failed (" + size + " requested). Dirty memory: " + page + ":" + offs);
				}
				pageBuf.putInt(offs, len);
				memUsed.addAndGet(len);			
				fence += 2;
				validate(pp);			
				return pp;
			}
		}
		
		// guarded by 'lock' field
		private int newPage(int start) {
			// code looks little awkward with off-heap allocator
			// refactoring required
			Page pageBuf = pageAllocator.allocate();
			pagesInUse.incrementAndGet();
			while(true) {
				int nextSlot = page(bumpPointer);
				for(int i = 0; i != pageUsageLimit; ++i) {
					int page = (nextSlot + i) % pageUsageLimit; 
					if (pages.get(page) == null) {
						if (pages.compareAndSet(page, null, pageBuf)) {
							pageBuf.setPageNo(page);
							pageBuf.markForAllocation();							
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

		@Override
		public String toString() {
			return name;
		}
	}
	
	
	static class Page {

		private static int STATUS_NEW = 0;
		private static int STATUS_ALLOCATING = 1;
		private static int STATUS_STORAGE = 2;
		private static int STATUS_FOR_EVACUATION = 3;
		private static int STATUS_RELEASED = 4;
		
		private static AtomicIntegerFieldUpdater<Page> MEM_USED = AtomicIntegerFieldUpdater.newUpdater(Page.class, "memUsed");
		private static AtomicIntegerFieldUpdater<Page> STATUS = AtomicIntegerFieldUpdater.newUpdater(Page.class, "status");

		private final OffHeapPageAllocator allocator;
		private final ByteBuffer bytes;
		private final int bufferId;
		
		private int pageNo = -1;
		// if true page is being used for allocating new chunks and should not be scavenged
		private volatile int status;
		private volatile int memUsed; 

		public Page(OffHeapPageAllocator allocator, ByteBuffer buffer, int bufferId) {
			this.allocator = allocator;
			this.bytes = buffer;
			this.bufferId = bufferId;
		}
		
		public int getPageNo() {
			return pageNo;
		}
		
		public void setPageNo(int pageNo) {
			this.pageNo = pageNo;
		}
		
		public boolean isForAllocation() {
			return status == STATUS_ALLOCATING;
		}
		
		public void markForAllocation() {
			STATUS.compareAndSet(this, STATUS_NEW, STATUS_ALLOCATING);
		}
		
		public void markForStorage() {
			STATUS.compareAndSet(this, STATUS_ALLOCATING, STATUS_STORAGE);
		}
		
		public boolean isMarkedForEvacuation() {
			return status == STATUS_FOR_EVACUATION;
		}
		
		public boolean markForEvacuation() {
			return STATUS.compareAndSet(this, STATUS_STORAGE, STATUS_FOR_EVACUATION);
		}
		
		public boolean markForRelease() {
			return STATUS.compareAndSet(this, STATUS_FOR_EVACUATION, STATUS_RELEASED);
		}
		
		public int getMemUsage() {
			return memUsed;
		}
		
		public void updateMemUsage(int delta) {
			MEM_USED.getAndAdd(this, delta);
		}
		
		public int intAt(int offs) {
			return bytes.getInt(offs);
		}
		
		public void putInt(int offs, int value) {
			bytes.putInt(offs, value);
		}

		public ByteChunk subChunk(int offs, int len) {
			byte[] chunk = new byte[len];
			ByteBuffer bb = bytes.duplicate();
			bb.position(bb.position() + offs);
			bb.get(chunk);
			return new ByteChunk(chunk);
		}
		
		public void putBytes(int offs, ByteChunk chunk) {
			ByteBuffer bb = bytes.duplicate();
			bb.position(bb.position() + offs);
			bb.put(chunk.array(), chunk.offset(), chunk.lenght());			
		}
		
		public void release() {
			allocator.release(bufferId);
		}
	}
	
	public void dumpPage(Page page) {
		System.err.println("Page dump, page " + page.getPageNo());
		int offs = PAGE_HEADER;
		while(offs < pageSize) {
			int size = page.intAt(offs);
			if (size == 0) {
				System.err.println("End of page 0x" + Integer.toHexString(offs) + "(" + offs + ")");
				break;
			}
			System.err.println("Chunk 0x" + Integer.toHexString(offs) + "(" + offs + ")" + ", size=" + size(size) + ", erased=" + erased(size));
			size = align(size(size));
			ByteChunk chunk = page.subChunk(offs, size);
			System.err.println(chunk.toString());
			offs += size;
		}
	}
}
