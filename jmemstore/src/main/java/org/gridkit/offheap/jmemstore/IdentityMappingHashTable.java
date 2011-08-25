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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;


/**
 * Class hashtable data structure, using explicit memory manager 
 *
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
class IdentityMappingHashTable implements IdentityMapping, MemoryConsumer {
	
		private static final int[] NO_ENTRIES = new int[0];

		// Entry structure
	    // | -- -- -- -- | -- -- -- -- | -- -- -- -- | -- -- -- -- | -- ... -- |
	    // | hash        | key size    | id          | ref count   | key data  |
	
		private final int POS_HASH = 0;
		private final int POS_KEY_SIZE = 4;
		private final int POS_ID = 8;
		private final int POS_REF_COUNT = 16;
		private final int POS_KEY_DATA = 20;
		
		static int ALLOC_NEW_ENTRY = 0;
		static int ALLOC_NEW_LIST = 0;
		static int ALLOC_RELOCATE_VALUE = 0;
		static int ALLOC_HASH_SEGMENT = 1;
	
		private final MemoryStoreBackend pageManager;
		
		private final int segmentCount;
		private int[] masterHashtable;
		private AtomicIntegerArray locktable;
		private volatile int capacity;
		
		private AtomicInteger size = new AtomicInteger();
		private float targetLoadFactor = 0.8f;
		
		private Object idLock = new String("idLock");
		private int upperBound;
		private int[] freeList = new int[1024];
		private int freeListL = 0;
		private int freeListR = 0;
		private int sweepPos;
		
		public IdentityMappingHashTable(MemoryStoreBackend pageManager, int segmentCount) {
			this.pageManager = pageManager;
			this.segmentCount = segmentCount;
			this.masterHashtable = new int[segmentCount];
			this.locktable = createLocktable(segmentCount);
			while(capacity < segmentCount) {
				increamentCapacity();
			}
		}

		private int allocateId() {
			synchronized(idLock) {
				while(true) {
					if (freeListL != freeListR) {
						int id =  freeList[freeListL];
						freeList[freeListL] = UNMAPPED;
						freeListL = (freeListL + 1) % freeList.length;
						if (id == UNMAPPED) {
							throw new AssertionError();
						}
						return id;
					}
					else {
						if (upperBound > (Integer.MAX_VALUE >> 1)) {
							scanForGaps(256);
							continue;
						}
						if ((size.get() / 2) < upperBound) {
							scanForGaps(8);
							if (freeListSize() > 0) {
								continue;
							}
							else {
								int id = upperBound;
								++upperBound;
								return id;
							}
						}
						else {
							int id = upperBound;
							++upperBound;
							return id;							
						}
					}
				}
			}
		}
		
		private void scanForGaps(int n) {
			scan_loop:
			while(true) {
				if (sweepPos >= upperBound || freeListSize() == (freeList.length - 1)) {
					sweepPos = 0;
					return;
				}
				int id = sweepPos;
				int nhash = BinHash.hash(id);
				sweepPos++;
				n++;
				int nindex = BinHash.splitHash(nhash, capacity);
				readLock(nindex);
				try {
					if (nindex != BinHash.splitHash(nhash, capacity)) {
						continue; // this will skip id check, but it should be ok
					}
					
					int[] entries = getEntries(nindex);
					if (entries != null) {
						for (int pp : entries) {
							if (getId(pp) == id) {
								continue scan_loop;
							}
						}
					}
					
					// no such id in table
					addToFreeList(id);
				}
				finally {
					readUnlock(nindex);
				}
			}
		}

		private void addToFreeList(int id) {
			freeList[freeListR] = id;
			freeListR = (freeListR + 1) % freeList.length;
			if (freeListL == freeListR) {
				throw new AssertionError();
			}
		}
		
		private int freeListSize() {
			int sz = freeListR - freeListL;
			if (sz < 0) {
				sz += freeList.length;
			}
			return sz;
		}

		@Override
		public int getIdByChunk(ByteChunk key) {
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
								return getId(entry);
							}
						}
					}
					return UNMAPPED;
				}
				finally {
					readUnlock(index);
				}
			}
		}

		@Override
		public ByteChunk getChunkById(int id) {
			while(true) {
				int hash = BinHash.hash(id);
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
							int entryId = getId(entry);
							if (entryId == id) {
								return getKeyData(entry);
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

		@Override
		public int map(ByteChunk key) {
			// Step I. Try increment ref counter on existing entry
			int hash = BinHash.hash(key);
			while(true) {
				int index = BinHash.splitHash(hash, capacity);
				writeLock(index);				
				try {
					if (index != BinHash.splitHash(hash, capacity)) {
						// table has been resized, try again
						continue;
					}
					int id = incRefCount(index, key);
					if (id != UNMAPPED) {
						return id;
					}
					else {
						break;
					}
				}
				finally {
					writeUnlock(index);
				}
			}
			// Step II. Create new entry and assign new ID.
			try {
				while(true) {
					int newId = allocateId();
					int idHash = BinHash.hash(newId);
					int hIndex = BinHash.splitHash(hash, capacity);
					int nIndex = BinHash.splitHash(idHash, capacity);
					writeDoubleLock(hIndex, nIndex);
					try {
						if (hIndex != BinHash.splitHash(hash, capacity) || nIndex != BinHash.splitHash(idHash, capacity)) {
							// table has been resized, try again
							continue;
						}
						
						int oldId = incRefCount(hIndex, key);
						if (oldId != UNMAPPED) {
							// somebody else have already created a mapping
							return oldId;
						}
						else {
							// adding new entry
							int npp = createEntry(key, newId, hash);
							addEntry(hIndex, npp);
							if (hIndex != nIndex) {
								addEntry(nIndex, npp);
							}
							size.addAndGet(2); // counting hash entries
							return newId;
						}
					}
					finally {
						writeDoubleUnlock(hIndex, nIndex);
					}
				}
			}
			finally {
				checkTableSize();
			}
		}

		// writeLock on index is assumed
		private int incRefCount(int index, ByteChunk key) {
			int[] entries = getEntries(index);
			if (entries != null) {
				for(int pp : entries) {
					ByteChunk entry = pageManager.get(pp);
					if (sameKey(entry, key)) {
						int refCount = getRefCount(entry);
						setRefCount(entry, refCount + 1);
						pageManager.update(pp, entry);
						// XXX
						if (key.toString().equals("[8ea5456f.c07d6f79.8bbbade2.30]")) {
							System.out.println(">>" + key + " refCount=" + pageManager.readInt(pp, POS_REF_COUNT));
						}
						return getId(entry);
					}
				}
			}
			return UNMAPPED;
		}

		@Override
		public void unmap(ByteChunk key) {
			// Step I. Try decrement ref counter on existing entry
			int hash = BinHash.hash(key);
			int id;
			while(true) {
				int index = BinHash.splitHash(hash, capacity);
				writeLock(index);				
				try {
					if (index != BinHash.splitHash(hash, capacity)) {
						// table has been resized, try again
						continue;
					}
					id = decRefCount(index, key);
					if (id == UNMAPPED) {
						return;
					}
					else {
						break;
					}
				}
				finally {
					writeUnlock(index);
				}
			}
			// Step II. Remove entry from hashtable
			while(true) {
				int idHash = BinHash.hash(id);
				int hIndex = BinHash.splitHash(hash, capacity);
				int nIndex = BinHash.splitHash(idHash, capacity);
				writeDoubleLock(hIndex, nIndex);
				try {
					if (hIndex != BinHash.splitHash(hash, capacity) || nIndex != BinHash.splitHash(idHash, capacity)) {
						// table has been resized, try again
						continue;
					}
					
					int[] entries = getEntries(nIndex);
					if (entries != null) {
						for(int pp : entries) {
							ByteChunk entry = pageManager.get(pp);
							if (sameKey(entry, key)) {
								int refCount = getRefCount(entry);
								if (refCount == 1) {
									pageManager.release(pp);
									removeEntry(nIndex, pp);
									removeEntry(hIndex, pp);
									size.addAndGet(-2); // size of hashtable
									return;
								}
								else {
									setRefCount(entry, refCount - 1);
									pageManager.update(pp, entry);
									//XXX
									if (key.toString().equals("[8ea5456f.c07d6f79.8bbbade2.30]")) {
										System.out.println("<<" + key + " refCount=" + pageManager.readInt(pp, POS_REF_COUNT));
									}
									return;
								}
							}
						}
					}
					throw new IllegalArgumentException("No mapping found for key " + key);
				}
				finally {
					writeDoubleUnlock(hIndex, nIndex);
				}
			}
		}
		
		@Override
		public void unmap(int id) {
			// Step I. Try decrement ref counter on existing entry
			int hash = 0;
			int ihash = BinHash.hash(id);
			step1:
			while(true) {
				int index = BinHash.splitHash(ihash, capacity);
				writeLock(index);				
				try {
					if (index != BinHash.splitHash(ihash, capacity)) {
						// table has been resized, try again
						continue;
					}

					int[] entries = getEntries(index);
					if (entries != null) {
						for(int pp : entries) {
							if (id == getId(pp)) {
								ByteChunk entry = pageManager.get(pp);
								int refCount = getRefCount(entry);
								if (refCount == 1) {
									hash = entry.intAt(POS_HASH);
									break step1;
								}
								setRefCount(entry, refCount - 1);
								pageManager.update(pp, entry);
								// XXX
								ByteChunk key = getKeyData(entry);
								if (key.toString().equals("[8ea5456f.c07d6f79.8bbbade2.30]")) {
									System.out.println("<<" + key + " refCount=" + pageManager.readInt(pp, POS_REF_COUNT));
								}
								return;
							}
						}
					}
					throw new IllegalArgumentException("No mapping found for id  " + id);
				}
				finally {
					writeUnlock(index);
				}
			}
			// Step II. Remove entry from hashtable
			while(true) {
				int nIndex = BinHash.splitHash(ihash, capacity);
				int hIndex = BinHash.splitHash(hash, capacity);
				writeDoubleLock(hIndex, nIndex);
				try {
					if (hIndex != BinHash.splitHash(hash, capacity) || nIndex != BinHash.splitHash(ihash, capacity)) {
						// table has been resized, try again
						continue;
					}
					
					int[] entries = getEntries(nIndex);
					if (entries != null) {
						for(int pp : entries) {
							if (id == getId(pp)) {
								ByteChunk entry = pageManager.get(pp);
								int refCount = getRefCount(entry);
								if (refCount == 1) {
									pageManager.release(pp);
									removeEntry(nIndex, pp);
									removeEntry(hIndex, pp);
									size.addAndGet(-2); // size of hashtable
									return;
								}
								else {
									setRefCount(entry, refCount - 1);
									pageManager.update(pp, entry);
									// XXX
									ByteChunk key = getKeyData(entry);
									if (key.toString().equals("[8ea5456f.c07d6f79.8bbbade2.30]")) {
										System.out.println("<<" + key + " refCount=" + pageManager.readInt(pp, POS_REF_COUNT));
									}
									return;
								}
							}
						}
					}
					throw new IllegalArgumentException("No mapping found for id " + id);
				}
				finally {
					writeDoubleUnlock(hIndex, nIndex);
				}
			}
		}

		/**
		 * @return UNMAPPED if ref counter > 0, pointer to chunk otherwise
		 */
		// writeLock on index is assumed
		private int decRefCount(int index, ByteChunk key) {
			int[] entries = getEntries(index);
			if (entries != null) {
				for(int pp : entries) {
					ByteChunk entry = pageManager.get(pp);
					if (sameKey(entry, key)) {
						int refCount = getRefCount(entry);
						if (refCount == 1) {
							return getId(entry);
						}
						setRefCount(entry, refCount - 1);
						pageManager.update(pp, entry);
						if (key.toString().equals("[8ea5456f.c07d6f79.8bbbade2.30]")) {
							System.out.println("<<" + key + " refCount=" + pageManager.readInt(pp, POS_REF_COUNT));
						}
						return UNMAPPED;
					}
				}
			}
			throw new IllegalArgumentException("No mapping found for key " + key);
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
				return NO_ENTRIES;
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
		
		// write lock for index assumed
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

		// write lock for index assumed
		private void addEntry(int index, int pp) {
			int[] entries = getEntries(index);
			if (entries == null) {
				setEntries(index, new int[]{pp});
			}
			else {
				int[] nentries = new int[entries.length + 1];
				for(int i = 0; i != entries.length; ++i) {
					nentries[i] = entries[i];
				}
				nentries[entries.length] = pp;
				setEntries(index, nentries);
			}
		}

		// write lock for index assumed
		private void removeEntry(int index, int pp) {
			int[] entries = getEntries(index);
			if (entries == null) {
				setEntries(index, new int[]{pp});
			}
			if (entries.length == 1) {
				if (entries[0] != pp) {
					// it is ok, just ignore 
//					throw new AssertionError("No such pointer in hash slot. Slot=" + index + ", pointer=" + pp);
				}
				else {
					setEntries(index, null);
				}
			}
			else {
				int[] nentries = new int[entries.length];
				int n = 0;
				for(int i = 0; i != entries.length; ++i) {
					if (entries[i] != pp) {
						nentries[n++] = entries[i];
					}
				}
				if (n != entries.length) {
					nentries = Arrays.copyOf(nentries, n);
					setEntries(index, nentries);
				}
			}
		}

		private int createEntry(ByteChunk key, int id, int hash) {
			int size  = POS_KEY_DATA + key.lenght();
			int npp = pageManager.allocate(size, ALLOC_NEW_ENTRY);
			ByteChunk chunk = pageManager.get(npp);
			try {
				chunk.assertEmpty();
			}
			catch(AssertionError e) {
				System.out.println("Problem pointer is " + pageManager.page(npp) + ":" + pageManager.offset(npp));
				throw e;
			}
		
			chunk.putInt(POS_HASH, hash);
			chunk.putInt(POS_KEY_SIZE, key.lenght());
			chunk.putInt(POS_ID, id);
			chunk.putInt(POS_REF_COUNT, 1);
			chunk.putBytes(POS_KEY_DATA, key);

			// no need for in-heap storage
			pageManager.update(npp, chunk);
			
			return npp;
		}
		
		private ByteChunk getKeyData(ByteChunk entry) {
			int size = entry.intAt(POS_KEY_SIZE);
			return entry.subChunk(POS_KEY_DATA, size);
		}
		
		private boolean sameKey(ByteChunk entry, ByteChunk key) {
			int keySize = entry.intAt(POS_KEY_SIZE);
			if (keySize == key.lenght()) {
				for (int i = 0; i != keySize; ++i) {
					if (entry.at(POS_KEY_DATA + i) != key.at(i)) {
						return false;
					}
				}
				return true;
			}
			else {
				return false;
			}
		}

		private int getId(ByteChunk entry) {
			return entry.intAt(POS_ID);
		}

		private int getId(int pp) {
			return pageManager.readInt(pp, POS_ID);
		}

		private int getRefCount(ByteChunk entry) {
			return entry.intAt(POS_REF_COUNT);
		}

		private void setRefCount(ByteChunk entry, int refCount) {
			entry.putInt(POS_REF_COUNT, refCount);
		}
		
		@Override
		public int size() {
			return size.get() / 2;
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
						if (pp != PagedBinaryStoreManager.EMPTY) {
							ByteChunk entry = pageManager.get(pp);
							int id = getId(entry);
							int nIndex = BinHash.splitHash(BinHash.hash(id), capacity);
							if (i == nIndex) {
								pageManager.release(pp);
								// there may be two pp in same entries array, so we have 
								// to zero them for avoiding double deallocating of memory chunk
								for(int x = 0; x != entries.length; ++x) {
									if (entries[x] == pp) {
										entries[x] = PagedBinaryStoreManager.EMPTY;
									}									
								}
							}
						}
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
			int evacuated = 0;
			for (int i = 0; i != hashCount; ++i) {
				int hash = evacuationHashes[i];
				evacuated += recycleHash(hash);
			}
			System.out.println("Evacuated " + evacuated + " bytes");
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
		
		int recycleHash(int hash) {
			int evacuated = 0;
			int[] recycleSet = null;
			int recycleCount = 0;
			while(true) {
				int cap = capacity;
				if (cap == 0) {
					// table is blank
					return evacuated;
				}
				int index = BinHash.splitHash(hash, cap); 
				readLock(index);
				try {
					if (BinHash.splitHash(hash, capacity) != index) {
						// capacity has been updated
						// need to recalculate index
						continue;
					}
					
					int[] entries = getEntries(index);
					
					if (entries == null || entries.length == 0) {
						return 0;
					}
					else {
						for(int i = 0; i != entries.length; ++i) {
							int pp = entries[i];
							int hIndex = BinHash.splitHash(pageManager.readInt(pp, POS_HASH), capacity);
							int nIndex = BinHash.splitHash(BinHash.hash(pageManager.readInt(pp, POS_ID)), capacity);
							if (needRecycle(pp) && pageManager.readInt(pp, POS_HASH) == hash) {
								if (recycleSet == null) {
									recycleSet = new int[entries.length - i];
								}
								recycleSet[recycleCount++] = getId(pp);
							}
						}
					}
					
					if (recycleCount == 0) {
						int hx = hashtableGet(index);
						if (hx < 0) {
							hx = -hx;
							if (needRecycle(hx)) {
								setEntries(index, entries);
								evacuated += 4 + 4 * entries.length;
							}
						}						
					}
				}
				finally {
					readUnlock(index);
				}
				break;
			}
			
			recycle_loop:
			for(int i  = 0; i != recycleCount; ++i) {
				int id = recycleSet[i];
								
				while(true) {
					int cap = capacity;
					if (cap == 0) {
						// table is blank
						return evacuated;
					}
					int idHash = BinHash.hash(id);
					int hIndex = BinHash.splitHash(hash, cap);
					int nIndex = BinHash.splitHash(idHash, cap);
					writeDoubleLock(hIndex, nIndex);
					try {
						if (hIndex != BinHash.splitHash(hash, capacity) || nIndex != BinHash.splitHash(idHash, capacity)) {
							// table has been resized, try again
							continue;
						}
						
						int[] hentries = getEntries(hIndex);
						int[] nentries = getEntries(nIndex);
						
						int pp = PagedBinaryStoreManager.EMPTY;
						for(int j = 0; j != nentries.length; ++j) {
							if (getId(nentries[j]) == id) {
								pp = nentries[j];
								break;
							}
						}
						
						if (needRecycle(pp)) {
							ByteChunk chunk = pageManager.get(pp);
							int cid = chunk.intAt(POS_ID);
							int chash = chunk.intAt(POS_HASH);
							if (id != cid || chash != hash) {
								// ignoring
								continue  recycle_loop;
							}
							// XXX
							if (getKeyData(chunk).toString().equals("[8ea5456f.c07d6f79.8bbbade2.30]")) {
								new String();
							}
							if (chunk.intAt(POS_HASH) != hash) {
								// actually it is possible due to race condition
								// such case should be ignored
								throw new AssertionError();
							}
							int np = pageManager.allocate(chunk.lenght(), ALLOC_RELOCATE_VALUE);
							ByteChunk newchunk = pageManager.get(np);
							newchunk.putBytes(chunk);
							pageManager.update(np, newchunk);
							pageManager.release(pp);
							
							for(int j = 0; j != hentries.length; ++j) {
								if (hentries[j] == pp) {
									hentries[j] = np;
								}
							}
							for(int j = 0; j != nentries.length; ++j) {
								if (nentries[j] == pp) {
									nentries[j] = np;
								}
							}
							
							evacuated += chunk.lenght();
							setEntries(hIndex, hentries);
							setEntries(nIndex, nentries);
						}

						// should check if collision list requires recycling
						int hx = hashtableGet(hIndex);
						if (hx < 0) {
							hx = -hx;
							if (needRecycle(hx)) {
								setEntries(hIndex, hentries);
								evacuated += 4 + 4 * hentries.length;
							}
						}
						int nx = hashtableGet(nIndex);
						if (nx < 0) {
							nx = -nx;
							if (needRecycle(nx)) {
								setEntries(nIndex, nentries);
								evacuated += 4 + 4 * nentries.length;
							}
						}
						break;
					}
					finally {
						writeDoubleUnlock(hIndex, nIndex);
					}
				}
			}
			return evacuated;
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
            for(int i = 0; i != n; ++i) {
            	int nRound = Integer.highestOneBit(capacity);
            	int nLast = capacity;
            	int nSplit = (nLast) & ~nRound;
            	writeDoubleLock(nSplit, nLast);
//	            checkHashConsistency();
            	try {
            		if (capacity == nLast) { 
	            		// writeLock(nLast) ensures what we are holding lock for capacity
	            		increamentCapacity(); // capacity increased to capacity + 1
	            		
		                int[] entries = getEntries(nSplit);
		                if (entries != null) {
		                	Arrays.sort(entries);
		                	int ppp = PagedBinaryStoreManager.EMPTY;
		                	
		                	int n1 = 0;
		                	int[] el1 = new int[entries.length];
		                	int n2 = 0;
		                	int[] el2 = new int[entries.length];
		                	
		                	for(int pp: entries) {
		                		// avoid processing of duplicated pointers
		                		if (ppp == pp) {
		                			continue;
		                		}
		                		ppp = pp;
		                		
		                		ByteChunk chunk = pageManager.get(pp);
		                		int hash = chunk.intAt(POS_HASH);
		                		int id = chunk.intAt(POS_ID);
		                		int ihash = BinHash.hash(id);
		                		
		                		boolean copied = false;
		                		int hIndex = BinHash.splitHash(hash, nLast); // old index
		                		int hhIndex = BinHash.splitHash(hash, nLast + 1);
		                		if (hIndex == nSplit) {
			                		if (hhIndex == nSplit) {
			                			el1[n1++] = pp;
			                		}
			                		else if (hhIndex == nLast) {
			                			el2[n2++] = pp;
			                		}
			                		else {
			                			throw new AssertionError("New index of hash " + Integer.toHexString(hash) + " is " + hhIndex + ", expected values eigther " + nSplit + " or " + nLast);
			                		}
			                		copied = true;
		                		}
		                		
		                		int nIndex = BinHash.splitHash(ihash, nLast);
		                		int nnIndex = BinHash.splitHash(ihash, nLast + 1);
		                		if (nIndex == nSplit) {
			                		if (nnIndex == nSplit && hhIndex != nSplit) {			                			
			                			el1[n1++] = pp;
			                		}
			                		else if (nnIndex == nLast && hhIndex != nLast) {
			                			el2[n2++] = pp;
			                		}
			                		else if (nnIndex != nSplit && nnIndex != nLast){
			                			throw new AssertionError("New index of hash " + Integer.toHexString(hash) + " is " + nnIndex + ", expected values eigther " + nSplit + " or " + nLast);
			                		}
			                		copied = true;
		                		}
		                		
		                		if (!copied) {
		                			throw new AssertionError("Entry does not belong to hash index");
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
//            		checkHashConsistency();
            		writeDoubleUnlock(nSplit, nLast);
            	}
            }
	    }

		@SuppressWarnings("unused") // for testing
	    private void checkHashConsistency() {
            for(int i = 0; i != capacity; ++i) {
            	int[] entries = getEntries(i);
            	if (entries != null) {
	            	for(int pp : entries) {
	            		ByteChunk entry = pageManager.get(pp);
	            		int hash = entry.intAt(POS_HASH);
	            		int ihash = BinHash.hash(entry.intAt(POS_ID));
	            		if (BinHash.splitHash(hash, capacity) != i && BinHash.splitHash(ihash, capacity) != i) {
	            			throw new AssertionError();
	            		}
	            	}
            	}
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
		
		public void _debug_dump() {
			for(int i = 0; i != capacity; ++i) {
				int[] entries = getEntries(i);
				if (entries != null && entries.length > 0) {
					System.out.print(i + "\t -> " + (entries == null ? "[]" : Arrays.toString(entries)));
					for(int pp : entries) {
						ByteChunk chunk = pageManager.get(pp);
						int hash = chunk.intAt(POS_HASH);
						int id = chunk.intAt(POS_ID);
						int refCount = chunk.intAt(POS_REF_COUNT);
						ByteChunk key = getKeyData(chunk);
						System.out.print(" #" + hash + " " + key + " id=" + id + " refCount=" + refCount);
					}					
					System.out.println();
				}
			}
		}
	}