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

import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public class PagedBinaryStoreManager implements BinaryStoreManager {

	static int EMPTY = 0;
	
	static int HASH_POS = 0;
	static int KEY_SIZE_POS = 4;
	static int VALUE_SIZE_POS = 8;
	static int DATA_POS = 12;
	
	static int ALLOC_NEW_VALUE = 0;
	static int ALLOC_NEW_LIST = 0;
	static int ALLOC_RELOCATE_VALUE = 0;
	static int ALLOC_HASH_SEGMENT = 1;
	
	private static long MEM_DIAG_REPORT_PERIOD = TimeUnit.SECONDS.toNanos(10);
	
	private final String name;
	private List<MemoryConsumer> tables = new ArrayList<MemoryConsumer>();
	private MemoryStoreBackend pageManager;
	private Thread maintenanceDaemon;
	
	public PagedBinaryStoreManager(String name, MemoryStoreBackend pageManager) {
		this.name = name;
		this.pageManager = pageManager;
		this.maintenanceDaemon = createMaintenanceThread();
	}
	
	private Thread createMaintenanceThread() {
		Thread thread = new Thread(new Runnable() {
			@Override
			public void run() {
				maintenanceCycle();
			}
		});
		thread.setName("PagedMemoryBinaryStore-" + name + "-ServiceThread");
		thread.setDaemon(true);
		return thread;
	}

	@Override
	public synchronized BinaryKeyValueStore createKeyValueStore() {
		BinaryHashTable hash = new BinaryHashTable(pageManager, 512);
		tables.add(hash);
		if (maintenanceDaemon.getState() == State.NEW) {
			maintenanceDaemon.start();
		}
		return hash;
	}
	
	@Override
	public synchronized BinaryKeyValueStore createKeyValueStore(int segments) {
		BinaryHashTable hash = new BinaryHashTable(pageManager, segments);
		tables.add(hash);
		if (maintenanceDaemon.getState() == State.NEW) {
			maintenanceDaemon.start();
		}
		return hash;
	}

	@Override
	public synchronized IdentityMapping createIdentityMapping() {
		IdentityMappingHashTable hash = new IdentityMappingHashTable(pageManager, 512);
		tables.add(hash);
		if (maintenanceDaemon.getState() == State.NEW) {
			maintenanceDaemon.start();
		}
		return hash;
	}
	
	@Override
	public synchronized void destroy(MemoryConsumer store) {
		// TODO check owner
		int n = tables.indexOf(store);
		tables.remove(n);
		store.destroy();
	}
	
	@SuppressWarnings("deprecation")
	public synchronized void close() {
		List<MemoryConsumer> tables = new ArrayList<MemoryConsumer>(this.tables);
		for(MemoryConsumer table: tables) {
			destroy(table);
		}
		if (maintenanceDaemon.getState() != State.NEW) {
			// TODO graceful death
			maintenanceDaemon.stop();
//			try {
//				maintenanceDaemon.join();
//			} catch (InterruptedException e) {
//				// ignore
//			}
		}
	}

	private void maintenanceCycle() {
		int n = 0;
		int idle = 0;
		long diagTimestamp = System.nanoTime();
		
		int[] evacuationHashes = new int[1024];
		
		MemoryConsumer[] tableSet = new MemoryConsumer[0];
		while(true) {
			
			if (n % 500 == 0) {
				synchronized(this) {
					tableSet = tables.toArray(tableSet);
				}
			}
			
			if (diagTimestamp + MEM_DIAG_REPORT_PERIOD < System.nanoTime()) {
				pageManager.dumpStatistics();
				synchronized (this) {
					int x = 0;
					for(MemoryConsumer consumer : tables) {
						StringBuilder buf = new StringBuilder();
//						buf.append("Hashtable #" + x).append("\n");
//						buf.append("Size: ").append(table.size()).append("\n");
//						buf.append("Capacity: ").append(table.getTableCapacity()).append("\n");
//						buf.append("Load factor: ").append(String.format("%f", 1.0d * table.size() / table.getTableCapacity())).append("\n");
//						buf.append("Hash slots usage: ").append(String.format("%f", 1.0d - 1.0d * table.getTableGapNumber() / table.getTableCapacity())).append("\n");

						if (consumer instanceof BinaryHashTable) {
							BinaryHashTable table = (BinaryHashTable) consumer;
							buf.append("Hashtable #" + x);
							buf.append(" | ");
							buf.append("Size: ").append(table.size());
							buf.append(" | ");
							buf.append("Capacity: ").append(table.getTableCapacity());
							buf.append(" | ");
							buf.append("Load factor: ").append(String.format("%f", 1.0d * table.size() / table.getTableCapacity()));
	//						buf.append(" | ");
	//						buf.append("Hash slots usage: ").append(String.format("%f", 1.0d - 1.0d * table.getTableGapNumber() / table.getTableCapacity()));
							System.out.println(buf.toString());
						}
						++x;
					}
				}
				diagTimestamp = System.nanoTime();
			}
		
			if (tableSet.length == 0) {
				++idle;
			}
			else {
				int len = pageManager.collectHashesForEvacuation(evacuationHashes, 0);
				if (len == 0) {
					++idle;
				}
				else {
					evacuateEntries(tableSet, evacuationHashes, len);
					Thread.yield();
				}
			}
			
			++n;
			
			if (idle > 10) {
				LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(300));
				idle = 0;
			}
		}
	}

	private void evacuateEntries(MemoryConsumer[] tableSet, int[] evacuationHashes, int hashCount) {
		for(MemoryConsumer table: tableSet) {
//			table.tableLock.readLock().lock();
			try {
				table.recycleHashes(evacuationHashes, hashCount);
			}
			finally {
//				table.tableLock.readLock().unlock();
			}
		}
		
	}
	
}
