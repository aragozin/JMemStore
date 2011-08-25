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

import java.util.Iterator;

/**
 * Key/List store is a store dedicated for inverted indexes.
 * Logically this a multimap, with values sorted within each key for paging retrieval.
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public interface BinaryKeyListStore {

	/**
	 * @return <code>true</code> is combination of key/value is present
	 */
	public boolean contains(ByteChunk key, ByteChunk value);
	
	/**
	 * Returns values associated with key. Returned values are optionally limited by range. Values are returned via provided array.
	 * @param key
	 * @param lowerBound lower bound for value range being returned (exclusive), <code>null</code> - unlimited
	 * @param upperBound upper bound for value range being returned (exclusive), <code>null</code> - unlimited
	 * @param values buffer for values being returned
	 * @return number of objects returned via values
	 */
	public int fetch(ByteChunk key, ByteChunk lowerBound, ByteChunk upperBound, ByteChunk[] values);
	
	/**
	 * @return number of value associated with key
	 */
	public int cordinality(ByteChunk key);
	
	public void append(ByteChunk key, ByteChunk value);
	
	public void remove(ByteChunk key);

	public void remove(ByteChunk key, ByteChunk value);
	
	public Iterator<ByteChunk> keys();
	
	public int size();
	
	public void clear();
	
}
