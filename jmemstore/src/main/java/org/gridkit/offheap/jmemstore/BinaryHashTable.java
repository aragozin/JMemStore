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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;


/**
 * Class hashtable data structure, using explicit memory manager 
 *
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
class BinaryHashTable implements BinaryKeyValueStore, MemoryConsumer {

		private final MemoryStoreBackend pageManager;
		
		private final int segmentCount;
		private int[] masterHashtable;
		private AtomicIntegerArray locktable;
		private volatile int capacity;
		
		private AtomicInteger size = new AtomicInteger();
		private float targetLoadFactor = 0.8f;
		
		public BinaryHashTable(MemoryStoreBackend pageManager, int segmentCount) {
			this.pageManager = pageManager;
			this.segmentCount = segmentCount;
			this.masterHashtable = new int[segmentCount];
			this.locktable = createLocktable(segmentCount);
			while(capacity < segmentCount) {
				increamentCapacity();
			}
		}
		
		private int hashtableGet(int index) {
			int slot = index % segmentCount;
			int ix = index / segmentCount;
			
			int pp = masterHashtable[slot];
			int value = pageManager.readInt(pp, ix * 4);
			return value;
		}
		
		private void hashtableSet(int index, int value) {
			int slot = index % segmentCount;
			int ix = index / segmentCount;
			
			int pp = masterHashtable[slot];
			pageManager.writeInt(pp, ix * 4, value);
		}
		
		// lock is assumed
		private int increamentCapacity() {
			// assumed newCap = capacity + 1
			
			int slot = capacity % segmentCount;
			int oldSegSize = alignSegmentSize(capacity / segmentCount);
			int newSegSize = alignSegmentSize(1 + (capacity / segmentCount));
			if (oldSegSize != newSegSize) {
				resizeSegment(slot, newSegSize);
			}
			return ++capacity;
		}
		
		private int getSegmentSize(int n) {
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
			--cap; // reserve one slot for memory manager
			return cap;
		}

		private void resizeSegment(int slot, int newSegSize) {
			int opp = masterHashtable[slot];
			int npp = pageManager.allocate(newSegSize * 4, PagedBinaryStoreManager.ALLOC_HASH_SEGMENT);
			if (opp != PagedBinaryStoreManager.EMPTY) {
				ByteChunk oldSeg = pageManager.get(opp);
				ByteChunk newSeg = pageManager.get(npp);
				newSeg.putBytes(oldSeg);
				// not required for in-heap backend
				pageManager.update(npp, newSeg);
				pageManager.release(opp);
			}
			masterHashtable[slot] = npp;
		}

		// lock assumed
		private int[] getEntries(int index) {
			int pointer;
			pointer = hashtableGet(index);
			if (pointer == 0) {
				return null;
			}
			else if (pointer > 0) {
				return new int[]{pointer};
			}
			else {
				pointer = -pointer;
				ByteChunk chunk = pageManager.get(pointer);
				int[] entries = new int[chunk.lenght() / 4 - 1];
				for(int i = 0; i != entries.length; ++i) {
					entries[i] = chunk.intAt(4 + i * 4);
				}
				return entries;
			}
		}
		
		// lock assumed
		private void setEntries(int index, int[] entries) {
			int pointer;
			pointer = hashtableGet(index);
			if (pointer != PagedBinaryStoreManager.EMPTY && pointer < 0) {
				pointer = -pointer;
				pageManager.release(pointer);
			}
			if (entries == null || entries.length == 0) {
				hashtableSet(index, PagedBinaryStoreManager.EMPTY);
			}
			else if (entries.length == 1) {
				hashtableSet(index, entries[0]);
			}
			else {
				ByteChunk first = pageManager.get(entries[0]);
				int hash = first.intAt(0);				
				int npp = pageManager.allocate(4 + 4 * entries.length, PagedBinaryStoreManager.ALLOC_NEW_LIST);
				ByteChunk list = pageManager.get(npp);
				try {
					list.assertEmpty();
				}
				catch(AssertionError e) {
					System.out.println("Problem pointer is " + pageManager.page(npp) + ":" + pageManager.offset(npp));
					throw e;
				}
				list.putInt(0, hash);
				for(int i = 0; i != entries.length; ++i) {
					list.putInt(4 + 4 * i, entries[i]);
				}
				// not required for in-heap backend
				pageManager.update(npp, list);
				hashtableSet(index, -npp);
			}
		}

		private void createEntry(int npp, ByteChunk key, ByteChunk value, int hash) {
			ByteChunk chunk = pageManager.get(npp);
			try {
				chunk.assertEmpty();
			}
			catch(AssertionError e) {
				System.out.println("Problem pointer is " + pageManager.page(npp) + ":" + pageManager.offset(npp));
				throw e;
			}
			chunk.putInt(PagedBinaryStoreManager.HASH_POS, hash);
			chunk.putInt(PagedBinaryStoreManager.KEY_SIZE_POS, key.lenght());
			chunk.putInt(PagedBinaryStoreManager.VALUE_SIZE_POS, value.lenght());
			chunk.putBytes(PagedBinaryStoreManager.DATA_POS, key);
			chunk.putBytes(PagedBinaryStoreManager.DATA_POS + key.lenght(), value);
		
			// no need for in-heap storage
			pageManager.update(npp, chunk);
		}

		private boolean sameKey(ByteChunk entry, ByteChunk key) {
			int keySize = entry.intAt(PagedBinaryStoreManager.KEY_SIZE_POS);
			if (keySize == key.lenght()) {
				for (int i = 0; i != keySize; ++i) {
					if (entry.at(PagedBinaryStoreManager.DATA_POS + i) != key.at(i)) {
						return false;
					}
				}
				return true;
			}
			else {
				return false;
			}
		}

		private boolean sameValue(ByteChunk entry, ByteChunk value) {
			int keySize = entry.intAt(PagedBinaryStoreManager.KEY_SIZE_POS);
			int valueSize = entry.intAt(PagedBinaryStoreManager.VALUE_SIZE_POS);
			if (valueSize == value.lenght()) {
				// TODO memcpy?
				for (int i = 0; i != valueSize; ++i) {
					if (entry.at(PagedBinaryStoreManager.DATA_POS + keySize + i) != value.at(i)) {
						return false;
					}
				}
				return true;
			}
			else {
				return false;
			}
		}

		private ByteChunk getKey(ByteChunk entry) {
			int keySize = entry.intAt(PagedBinaryStoreManager.KEY_SIZE_POS);
			return entry.subChunk(PagedBinaryStoreManager.DATA_POS, keySize);
		}

		private ByteChunk getValue(ByteChunk entry) {
			int keySize = entry.intAt(PagedBinaryStoreManager.KEY_SIZE_POS);
			int valueSize = entry.intAt(PagedBinaryStoreManager.VALUE_SIZE_POS);
			return entry.subChunk(PagedBinaryStoreManager.DATA_POS + keySize, valueSize);
		}

		@Override
		public int size() {
			return size.get();
		}

		@Override
		public ByteChunk get(ByteChunk key) {
//			tableLock.readLock().lock();
			try {
				while(true) {
					int hash = BinHash.hash(key);
					int index = BinHash.splitHash(hash, capacity);
					readLock(index);				
					try {
						if (index != BinHash.splitHash(hash, capacity)) {
							continue;
						}
						int[] entries = getEntries(index);
						if (entries != null) {
							for(int pp : entries) {
								ByteChunk entry = pageManager.get(pp);
								if (sameKey(entry, key)) {
									return getValue(entry);
								}
							}
						}
						return null;
					}
					finally {
						readUnlock(index);
					}
				}
			}
			finally {
//				tableLock.readLock().unlock();
			}
		}

		@Override
		public void put(ByteChunk key, ByteChunk value) {
			boolean inserted = internalPut(key, value, false, null);
			if (inserted) {
				checkTableSize();
			}
		}
		
		

		@Override
		public boolean compareAndPut(ByteChunk key, ByteChunk expected, ByteChunk newValue) {
			boolean inserted = internalPut(key, newValue, true, expected);
			if (inserted && expected == null) {
				checkTableSize();
			}
			return inserted;
		}

		private boolean internalPut(ByteChunk key, ByteChunk value, boolean checkOldValue, ByteChunk expected) {
		
			while(true) {
				int hash = BinHash.hash(key);
				int index = BinHash.splitHash(hash, capacity);
				writeLock(index);
				try {			
					if (index != BinHash.splitHash(hash, capacity)) {
						continue;
					}
					
					int[] entries = getEntries(index);
					
					if (entries != null) {
						for(int i = 0; i != entries.length; ++i) {
							int pp = entries[i];
							ByteChunk entry = pageManager.get(pp);
							if (sameKey(entry, key)) {
								
								if (checkOldValue) {
									if (expected == null && !sameValue(entry, expected)) {
										return false;
									}
								}
								
								// overriding value
								pageManager.release(pp);
								int npp = pageManager.allocate(PagedBinaryStoreManager.DATA_POS + key.lenght() + value.lenght(), PagedBinaryStoreManager.ALLOC_NEW_VALUE);
								createEntry(npp, key, value, hash);
								entries[i] = npp;
								setEntries(index, entries);
								return checkOldValue ? true : false;
							}
						}
					}
						
					// TODO refactoring, move allocation to createEntry method
					if (checkOldValue) {
						if (expected != null) {
							return false;
						}
					}
					
					// add new entry
					int npp = pageManager.allocate(PagedBinaryStoreManager.DATA_POS + key.lenght() + value.lenght(), PagedBinaryStoreManager.ALLOC_NEW_VALUE);
					createEntry(npp, key, value, hash);
		
					int[] newEntries;
					if (entries == null || entries.length == 0) {
						newEntries = new int[]{npp};
					}
					else {
						newEntries = Arrays.copyOf(entries, entries.length + 1);
						newEntries[entries.length] = npp;
					}
					
					setEntries(index, newEntries);
					size.incrementAndGet();
					return true;
				}
				finally {
					writeUnlock(index);
				}
			}
		}

		@Override
		public void remove(ByteChunk key) {
			internalRemove(key, null);
		}

		
		@Override
		public boolean compareAndRemove(ByteChunk key, ByteChunk expected) {
			if (expected != null) {
				return internalRemove(key, expected);
			}
			else {
				return false;
			}
		}

		private boolean internalRemove(ByteChunk key, ByteChunk expected) {
			while(true) {
				int hash = BinHash.hash(key);
				int index = BinHash.splitHash(hash, capacity);
				writeLock(index);
				try {
					if (index != BinHash.splitHash(hash, capacity)) {
						continue;
					}
					
					int[] entries = getEntries(index);
					
					if (entries != null) {
						for(int pp : entries) {
							ByteChunk entry = pageManager.get(pp);
							if (sameKey(entry, key)) {
								
								if (expected != null) {
									if (!sameValue(entry, expected)) {
										return false;
									}
								}
								
								pageManager.release(pp);
								if (entries.length == 1) {
									setEntries(index, null);
								}
								else {
									int[] newEntries = new int[entries.length - 1];
									int n = 0;
									for(int pi :  entries) {
										if (pi != pp) {
											newEntries[n++] = pi;
										}
									}
									setEntries(index, newEntries);
								}
								size.decrementAndGet();
								return true;
							}
						}
					}
					// not found
					return false;
				}
				finally {
					writeUnlock(index);
				}
			}
		}

		@Override
		public Iterator<ByteChunk> keys() {
			return new HashIterator();
		}

		@Override
		public void clear() {
			clear(true);			
		}

		@Override
		public void destroy() {
			clear(false);			
		}

		// TODO check clear method
		void clear(boolean reinit) {
			// lock everything
			for(int i = 0; i != segmentCount; ++i) {
				segmentWriteLock(i);
			}
			int[] empty = new int[0];
			for(int i = 0; i != capacity; ++i) {
				int[] entries = getEntries(i);
				if (entries != null) {
					for(int pp : entries) {
						pageManager.release(pp);
					}
					setEntries(i, empty);
				}
			}
			capacity = 0;
			size.set(0);
			
			for(int i = 0; i != segmentCount; ++i) {
				int pp = masterHashtable[i];
				if (pp != PagedBinaryStoreManager.EMPTY) {
					pageManager.release(pp);
					masterHashtable[i] = 0;
				}
			}
			
			if (reinit) {
				while(capacity < segmentCount) {
					increamentCapacity();
				}
			}
			
			// unlock, unlock order does not matter
			for(int i = 0; i != segmentCount; ++i) {
				segmentWriteUnlock(i);
			}				
		}
		
		public int getTableCapacity() {
			return capacity;
		}
		
		public double getTargetLoadFactor() {
			return targetLoadFactor;
		}
		
		public int getTableGapNumber() {
			int n = 0;
			for(int i = 0; i < capacity; ++i) {
				readLock(i);
				try {
					if (hashtableGet(i) == 0) {
						++n;
					}
				}
				finally{
					readUnlock(i);
				}
			}
			return n;
		}
		
		public void recycleHashes(int[] evacuationHashes, int hashCount) {
			for (int i = 0; i != hashCount; ++i) {
				int hash = evacuationHashes[i];
				recycleHash(hash);
			}
			recycleHashtable();
		}
		
		// TODO slow pace recycling
		void recycleHashtable() {
			for(int i = 0; i != segmentCount; ++i) {
				int pp = masterHashtable[i];
				if (needRecycle(pp)) {
					segmentWriteLock(i);
					try {
						pp = masterHashtable[i];
						if (needRecycle(pp)) {
							int segSize = getSegmentSize(i);
//							System.out.println("Recycling hash segment " + pageManager.page(pp) + ":" + pageManager.offset(pp));
							resizeSegment(i, segSize);
						}
					}
					finally {
						segmentWriteUnlock(i);
					}
				}
			}
		}
		
		// tableLock assumed
		void recycleHash(int hash) {
			while(true) {
				int index = BinHash.splitHash(hash, capacity); 
				writeLock(index);
				try {
					if (BinHash.splitHash(hash, capacity) != index) {
						// capacity has been updated
						// need to recalculate index
						continue;
					}
					
					int[] entries = getEntries(index);
					
					if (entries != null && entries.length > 0) {
						boolean modified = false;
						for(int i = 0; i != entries.length; ++i) {
							int pp = entries[i];
							if (needRecycle(pp)) {
								ByteChunk chunk = pageManager.get(pp);
								int npp = pageManager.allocate(chunk.lenght(), PagedBinaryStoreManager.ALLOC_RELOCATE_VALUE);
								ByteChunk newChunk = pageManager.get(npp);
								newChunk.putBytes(chunk);
								pageManager.release(pp);
								// not required for in-heap storage
								pageManager.update(npp, newChunk);
								entries[i] = npp;
								modified = true;
							}
						}
						
						if (!modified) {
							int pe = hashtableGet(index);
							pe = pe > 0 ? pe : -pe;
							if (needRecycle(pe)) {
								modified = true;
							}
						}
						
						if (modified) {
							setEntries(index, entries);
						}
					}
				}
				finally {
					writeUnlock(index);
				}
				break;
			}
		}
		
		private boolean needRecycle(int pointer) {
			return pointer != PagedBinaryStoreManager.EMPTY && pageManager.isMarkedForRecycle(pointer);
		}

		private void checkTableSize() {			
			float loadFactor = ((float)size.get()) / capacity;
			if (loadFactor > targetLoadFactor) {
				// grow by 1.5
				if (capacity % 2 == 0) {
					growTable(2);
				}
				else {
					growTable(1);
				}
			}
		}
		
		private void growTable(int n) {
//			tableLock.readLock().lock();
			try {
	            for(int i = 0; i != n; ++i) {
	            	int nRound = Integer.highestOneBit(capacity);
	            	int nLast = capacity;
	            	int nSplit = (nLast) & ~nRound;
	            	writeDoubleLock(nSplit, nLast);
//	            	checkHashConsistency();
	            	try {
	            		if (capacity == nLast) { 
	            			int originalCapacity = capacity;
		            		// writeLock(nLast) ensures what we are holding lock for capacity
		            		increamentCapacity(); // capacity increased to capacity + 1
		            		
			                int[] entries = getEntries(nSplit);
			                if (entries != null) {
			                	int n1 = 0;
			                	int[] el1 = new int[entries.length];
			                	int n2 = 0;
			                	int[] el2 = new int[entries.length];
			                	
			                	for(int pp: entries) {
			                		ByteChunk chunk = pageManager.get(pp);
			                		int hash = chunk.intAt(PagedBinaryStoreManager.HASH_POS);
			                		int index = BinHash.splitHash(hash, nLast + 1);
			                		if (index == nSplit) {
			                			el1[n1++] = pp;
			                		}
			                		else if (index == nLast) {
			                			el2[n2++] = pp;
			                		}
			                		else {
			                			System.err.println("[[ Problem in 'growTable' - Thread:" + Thread.currentThread().toString());
			                			System.err.println("New index of hash " + Integer.toHexString(hash) +" is " + index + ", expected values eigther " + nSplit + " or " + nLast);
			                			System.err.println("Original capacity: " + originalCapacity + " hash index " + BinHash.splitHash(hash, originalCapacity));
			                			System.err.println("Current capacity: " + capacity + " hash index " + Integer.toHexString(hash));
			                			System.err.println("]]");
			                			throw new AssertionError("New index of hash " + Integer.toHexString(hash) + " is " + index + ", expected values eigther " + nSplit + " or " + nLast);
			                		}
			                	}
			                	el1 = Arrays.copyOf(el1, n1);
			                	el2 = Arrays.copyOf(el2, n2);
			                	
			                	setEntries(nSplit, el1);
			                	setEntries(nLast, el2);
			                }
	            		}
	            	}
	            	finally {
//		            	checkHashConsistency();
	            		writeDoubleUnlock(nSplit, nLast);
	            	}
	            }
			}
            finally {
//            	tableLock.readLock().unlock();
            }
	    }

		@SuppressWarnings("unused") // for testing
	    private void checkHashConsistency() {
//	        tableLock.readLock().lock();
	        try {
	            for(int i = 0; i != capacity; ++i) {
	            	int[] entries = getEntries(i);
	            	if (entries != null) {
		            	for(int pp : entries) {
		            		ByteChunk entry = pageManager.get(pp);
		            		int hash = entry.intAt(PagedBinaryStoreManager.HASH_POS);
		            		if (BinHash.splitHash(hash, capacity) != i) {
		            			throw new AssertionError();
		            		}
		            	}
	            	}
	            }            
	        }
	        finally {
//	            tableLock.readLock().unlock();
	        }
	    }
	    
//		private int hashIndex(ByteChunk key, int capacity) {
//	        int hash = BinHash.hash(key);
//	        return PagedBinaryStoreManager.splitHash(hash, capacity);
//	    }
		
		private AtomicIntegerArray createLocktable(int size) {
			AtomicIntegerArray table = new AtomicIntegerArray(size / 4); // 8 bits per lock
			return table;
		}
		
		private void readLock(int index) {
			int seg = index % segmentCount;
			segmentReadLock(seg);
		}
		
		private void readUnlock(int index) {
			int seg = index % segmentCount;
			segmentReadUnlock(seg);
		}
		
		private void segmentReadLock(int index) {
			int n = 0;
			while(true) {
				byte c = byte_get(locktable, index);
				if (c >= 0 && c < 126) {
					byte u = (byte) (c + 1) ;
					if (byte_compareAndSet(locktable, index, c, u)) {
						return;
					}
				}				
				++n;
				if (n % 10 == 0) {
					Thread.yield();
				}
			}
		}

		private void segmentReadUnlock(int index) {
			int n = 0;
			while(true) {
				byte c = byte_get(locktable, index);
				if (c > 0) {
					byte u = (byte) (c - 1) ;
					if (byte_compareAndSet(locktable, index, c, u)) {
						return;
					}
				}				
				else if (c < 0) {
					byte u = (byte) (c + 1);
					if (byte_compareAndSet(locktable, index, c, u)) {
						return;
					}
				}
				else {
					throw new IllegalStateException("Invalid lock state");
				}
				++n;
				if (n % 10 == 0) {
					Thread.yield();
				}
			}
		}
		
		
		private void writeLock(int index) {
			int segment = index % segmentCount;
			segmentWriteLock(segment);
		}
		
		private void writeUnlock(int index) {
			int segment = index % segmentCount;
			segmentWriteUnlock(segment);
		}

		private void writeDoubleLock(int index1, int index2) {
			int seg1 = index1 % segmentCount;
			int seg2 = index2 % segmentCount;

			if (seg1 > seg2) {
				int t = seg1;
				seg1 = seg2;
				seg2 = t;
			}
			
			segmentWriteLock(seg1);
			if (seg1 != seg2) {
				segmentWriteLock(seg2);
			}
		}

		private void writeDoubleUnlock(int index1, int index2) {
			int seg1 = index1 % segmentCount;
			int seg2 = index2 % segmentCount;

			if (seg1 > seg2) {
				int t = seg1;
				seg1 = seg2;
				seg2 = t;
			}
			
			if (seg1 != seg2) {
				segmentWriteUnlock(seg2);
			}
			segmentWriteUnlock(seg1);
		}		
		
		private void segmentWriteLock(int index) {
			int n = 0;
			while(true) {
				byte c = byte_get(locktable, index);
				if (c == 0) {
					byte u = (byte) -1;
					if (byte_compareAndSet(locktable, index, c, u)) {
						return;
					}
				}				
				else if (c < 0) {
					// another writer is pending					
				}
				else if (c > 0){
					byte u = (byte) (-c - 1);
					if (byte_compareAndSet(locktable, index, c, u)) {
						break;
					}
				}
				++n;
				if (n % 10 == 0) {
					Thread.yield();
				}
			}
			// waiting read locks to get released
			while(true) {
				byte c = byte_get(locktable, index);
				if (c == -1) {
					return;
				}				

				++n;
				if (n % 10 == 0) {
					Thread.yield();
				}				
			}			
		}

		private void segmentWriteUnlock(int index) {
			int n = 0;
			while(true) {
				byte c = byte_get(locktable, index);
				if (c == -1) {
					byte u = (byte) 0;
					if (byte_compareAndSet(locktable, index, c, u)) {
						return;
					}
				}				
				else {
					throw new IllegalStateException("Broken lock");
				}
				++n;
				if (n % 10 == 0) {
					Thread.yield();
				}
			}
		}
		
		private byte byte_get(AtomicIntegerArray table, int index) {
			int x = index / 4;
			int xx = index % 4;			
			int word = table.get(x);
			return getByte(word, xx);
		}

		private boolean byte_compareAndSet(AtomicIntegerArray table, int index, byte expected, byte newValue) {
			int x = index / 4;
			int xx = index % 4;
			
			while(true) {
				int word = table.get(x);
				byte val = getByte(word, xx);
				if (val == expected) {
					int newWord = setByte(word, xx, newValue);
					if (table.compareAndSet(x, word, newWord)) {
						return true;
					}
					else {
						continue;
					}
				}
				else {
					return false;
				}				
			}			
		}
		
		private byte getByte(int word, int i) {
			switch(i) {
			case 0:
				return (byte) (0xFF & word);
			case 1:
				return (byte) (0xFF & (word >> 8));				
			case 2:
				return (byte) (0xFF & (word >> 16));				
			case 3:
				return (byte) (0xFF & (word >> 24));				
			default:
				throw new IllegalArgumentException("4 bytes per int");
			}			
		}
		
		private int setByte(int word,int i, byte value) {
			switch(i) {
			case 0:
				word &= 0xFFFFFF00;
				word |= 0xFF & (int)value;
				return word;
			case 1:
				word &= 0xFFFF00FF;
				word |= (0xFF & (int)value) << 8;
				return word;				
			case 2:
				word &= 0xFF00FFFF;
				word |= (0xFF & (int)value) << 16;
				return word;				
			case 3:
				word &= 0x00FFFFFF;
				word |= (0xFF & (int)value) << 24;
				return word;				
			default:
				throw new IllegalArgumentException("4 bytes per int");
			}
		}
		
		private class HashIterator implements Iterator<ByteChunk> {
			
			private int position = 0;
			private final List<ByteChunk> buffer = new ArrayList<ByteChunk>();
			
			public HashIterator() {
				feed();
			}

			private void feed() {
				readLock(position);
				try {
					int[] entries = getEntries(position);
					if (entries != null) {
						for(int pp : entries) {
							ByteChunk entry = pageManager.get(pp);
							buffer.add(getKey(entry));
						}
					}
				}
				finally{
					readUnlock(position);
				}
			}
			
			@Override
			public boolean hasNext() {
				while(buffer.isEmpty()) {
					++position;
					if (position >= capacity) {
						return false;
					}
					else {
						feed();
					}
				}
				return true;
			}

			@Override
			public ByteChunk next() {
				if (hasNext()) { 
					return buffer.remove(0);
				}
				else {
					throw new NoSuchElementException();
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();			
			}
		}		
	}