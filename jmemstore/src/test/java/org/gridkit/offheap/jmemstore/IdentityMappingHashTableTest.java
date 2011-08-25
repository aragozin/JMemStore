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

import org.gridkit.offheap.jmemstore.IdentityMapping;
import org.gridkit.offheap.jmemstore.InHeapMemoryStoreBackend;
import org.gridkit.offheap.jmemstore.MemoryConsumer;
import org.gridkit.offheap.jmemstore.PagedBinaryStoreManager;
import org.junit.Test;

/**
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public class IdentityMappingHashTableTest {

	static InHeapMemoryStoreBackend pageManager = new InHeapMemoryStoreBackend(64 << 10, 256, 2);
	static PagedBinaryStoreManager storeMan = new PagedBinaryStoreManager("IdentityMappingHashTableTest", pageManager);
	
	@Test
	public void complexTest() {
		
		IdentityMapping mapping = storeMan.createIdentityMapping();
		
		RandomIdentityMappingTester tester = new RandomIdentityMappingTester();
		
		tester.start(mapping);
		storeMan.destroy((MemoryConsumer) mapping);
		
	}
	
	@Test
	public void longTest() {
		
		IdentityMapping mapping = storeMan.createIdentityMapping();
		
		RandomIdentityMappingTester tester = new RandomIdentityMappingTester();
		tester.variety = 5000;
		tester.populationPhase = 10000;
		tester.mainPhase = 100000;
		tester.removalPhase = 20000;
		
		tester.start(mapping);
		storeMan.destroy((MemoryConsumer) mapping);
		
	}
}
