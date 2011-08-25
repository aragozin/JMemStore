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


/**
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
interface MemoryStoreBackend {

	public abstract ByteChunk get(int pointer);
	
	public abstract void update(int pointer, ByteChunk bytes);

	public abstract int allocate(int size, int allocNo);

	public abstract void release(int pointer);

	public abstract int collectHashesForEvacuation(int[] hashes, int len);

	public abstract boolean isMarkedForRecycle(int pp);

	public abstract long getMemUsage();

	public abstract void dumpStatistics();

	// for diagnostic reasons
	public abstract int page(int npp);

	// for diagnostic reasons
	public abstract int offset(int npp);

	public int readInt(int pointer, int offset);

	public void writeInt(int pointer, int offset, int value);

}