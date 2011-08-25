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
public interface IdentityMapping {

	public static final int UNMAPPED = -1;
	
	public int map(ByteChunk chunk);
	
	public int getIdByChunk(ByteChunk chunk);
	
	public ByteChunk getChunkById(int id);
	
	public void unmap(ByteChunk chunk);
	
	public void unmap(int id);
	
	public int size();
	
	public void clear();
	
}
