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

import org.gridkit.offheap.jmemstore.BinaryStoreManager;
import org.gridkit.offheap.jmemstore.InHeapMemoryStoreBackend;
import org.gridkit.offheap.jmemstore.PagedBinaryStoreManager;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public class PagedMemoryBinaryStoreManager2Test extends BaseBinaryStoreManagerTest {

	@Test @Override
	public void test_basics() {
		// TODO Auto-generated method stub
		super.test_basics();
	}

	@Test @Override
	public void large_consistency_test_basics() {
		// TODO Auto-generated method stub
		super.large_consistency_test_basics();
	}

	@Ignore @Override
	protected BinaryStoreManager createLargeStoreManager() {
		InHeapMemoryStoreBackend pageManager = new InHeapMemoryStoreBackend(64 << 10, 256, 2);
		
		PagedBinaryStoreManager storeMan = new PagedBinaryStoreManager("test_basics", pageManager);
		return storeMan;
	}

	@Ignore @Override
	protected BinaryStoreManager createSmallPageManager() {
		InHeapMemoryStoreBackend pageManager = new InHeapMemoryStoreBackend(8 << 10, 16, 2);
		
		PagedBinaryStoreManager storeMan = new PagedBinaryStoreManager("test_basics", pageManager);
		return storeMan;
	}

	
	
}
