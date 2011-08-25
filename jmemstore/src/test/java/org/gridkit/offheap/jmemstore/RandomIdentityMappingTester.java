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

import java.util.Random;

import org.gridkit.offheap.jmemstore.ByteChunk;
import org.gridkit.offheap.jmemstore.IdentityMapping;
import org.gridkit.offheap.jmemstore.IdentityMappingHashTable;
import org.junit.Assert;
import org.junit.Ignore;

/**
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
@Ignore
public class RandomIdentityMappingTester {
	
	public Random rnd = new Random(0);
	private Random rnd2 = new Random();
	
	public int variety = 1000;
	public int populationPhase = 10000;
	public int mainPhase = 50000;
	public int removalPhase = 20000;
	
	
	public void start(IdentityMapping mapping) {
		
		try {
		
			SimpleIdentityMapping refMap = new SimpleIdentityMapping();
	
			int n = 0;
			// population
			while(n < populationPhase) {
				
				if (probability(0.8)) {
					ByteChunk key = randomBytes(nextKey());
					
					if (key.toString().equals("[8ea5456f.c07d6f79.8bbbade2.30]")) {
						System.out.println("++" + key + " at " + refMap.cardinality(key));
					}

					int id = mapping.map(key);

					if (key.toString().equals("[8bea12ce.b9527aae.11377490.f1]")) {
						new String(); // ((IdentityMappingHashTable)mapping)._debug_dump();
					}
					refMap.map(key, id);
				}
				else {
					while(true) {
						ByteChunk key = randomBytes(nextKey());
						if (key.toString().equals("[8bea12ce.b9527aae.11377490.f1]")) {
							new String();
						}
						int id = refMap.getIdByChunk(key);
						if (id == IdentityMapping.UNMAPPED) {
							continue;
						}
						
						if (key.toString().equals("[8ea5456f.c07d6f79.8bbbade2.30]")) {
							System.out.println("--" + key + " at " + refMap.cardinality(key));
						}
						
						if (probability(0.5)) {
							mapping.unmap(id);
							refMap.unmap(id);
						}
						else {
							mapping.unmap(key);
							refMap.unmap(key);
						}
						break;
					}
				}
				
				++n;
				if (n % 100 == 0) {
					compare(mapping, refMap);
				}
				Assert.assertEquals(refMap.size(), mapping.size());
			}
	
			n = 0;
			// population
			while(n < mainPhase) {
				
				if (probability(0.5)) {
					ByteChunk key = randomBytes(nextKey());

					if (key.toString().equals("[8ea5456f.c07d6f79.8bbbade2.30]")) {
						System.out.println("++" + key + " at " + refMap.cardinality(key));
					}

					int id = mapping.map(key);
					refMap.map(key, id);
					Assert.assertEquals(refMap.size(), mapping.size());
				}
				else {
					while(true) {
						ByteChunk key = randomBytes(nextKey());
						int id = refMap.getIdByChunk(key);
						if (id == IdentityMapping.UNMAPPED) {
							continue;
						}
						
						if (key.toString().equals("[8ea5456f.c07d6f79.8bbbade2.30]")) {
							System.out.println("--" + key + " at " + refMap.cardinality(id));							
						}
						if (probability(0.5)) {
							mapping.unmap(id);
							refMap.unmap(id);
							if (refMap.size() != mapping.size()) {
								Assert.assertEquals(refMap.size(), mapping.size());
							}
						}
						else {
							mapping.unmap(key);
							refMap.unmap(key);
							Assert.assertEquals(refMap.size(), mapping.size());
						}
						break;
					}
				}
				
				++n;
				if (n % 100 == 0) {
					compare(mapping, refMap);
				}
				Assert.assertEquals(refMap.size(), mapping.size());
			}
	
			n = 0;
			// removal phase
			while(n < removalPhase) {
				
				if (refMap.size() == 0) {
					break;
				}
				
				if (probability(0.2)) {
					ByteChunk key = randomBytes(nextKey());
					int id = mapping.map(key);
					refMap.map(key, id);
				}
				else {
					while(true) {
						
						ByteChunk key;
						int id;
						if (probability(0.8)) {
							key = randomBytes(nextKey());
							id = refMap.getIdByChunk(key);
							if (id == IdentityMapping.UNMAPPED) {
								continue;
							}
						}
						else {
							id = refMap.getValidId();
							key = refMap.getChunkById(id);
						}
						
						if (probability(0.5)) {
							mapping.unmap(id);
							refMap.unmap(id);
						}
						else {
							mapping.unmap(key);
							refMap.unmap(key);
						}
						break;
					}
				}
				
				++n;
				if (n % 100 == 0) {
					compare(mapping, refMap);
				}
				Assert.assertEquals(refMap.size(), mapping.size());
			}
	
			compare(mapping, refMap);
		}
		catch(RuntimeException e) {
			((IdentityMappingHashTable)mapping)._debug_dump();
			throw e;
		}
		catch(AssertionError e) {
			((IdentityMappingHashTable)mapping)._debug_dump();
			throw e;
		}
	}

	public boolean probability(double x) {
		return rnd.nextDouble() < x;
	}
	
	public int nextKey() {
		return rnd.nextInt(variety);
	}
	
	public ByteChunk randomBytes(int n) {
		rnd2.setSeed(n);
		int len = 8 + rnd2.nextInt(8);
		byte[] chunk = new byte[len];
		rnd2.nextBytes(chunk);
		return new ByteChunk(chunk);
	}
	
	public void compare(IdentityMapping mapping, SimpleIdentityMapping refMapping) {		
		for(int i = 0; i != variety; ++i) {
			ByteChunk key = randomBytes(i);
			int id = refMapping.getIdByChunk(key);
			
			Assert.assertEquals(id, mapping.getIdByChunk(key));
			if (id != IdentityMapping.UNMAPPED) {
				Assert.assertEquals(key.toString(), String.valueOf(mapping.getChunkById(id)));
			}
		}
		Assert.assertEquals(mapping.size(), refMapping.size());
	}
}
