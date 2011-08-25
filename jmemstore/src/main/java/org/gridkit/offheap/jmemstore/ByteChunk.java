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

/**
 * A kind of {@link ByteBuffer}, byte buffer itself is a bit too complicated
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 *
 */
public final class ByteChunk {

	private final byte[] bytes;
	private final int offset;
	private final int len;
	
	public ByteChunk(byte[] bytes) {
		this.bytes = bytes;
		this.offset = 0;
		this.len = bytes.length;
	}
	
	public ByteChunk(byte[] bytes, int offset, int len) {
		this.bytes = bytes;
		this.offset = offset;
		this.len = len;
	}

	public byte[] array() {
		return bytes;
	}
	
	public int offset() {
		return offset;
	}
	
	public int lenght() {
		return len;
	}

	public byte at(int i) {
		return bytes[offset + i];
	}

	public void set(int i, byte b) {
		bytes[offset + i] = b;
	}

	public ByteChunk subChunk(int offs, int len) {
		if (offs + len > this.len) {
			throw new IllegalArgumentException("Chunk " + bytes + " offs: " + offset + " len: " + this.len + ". Required subrange " + offs + " by " + len + "(" + Integer.toHexString(len) + ")");			
		}
		return new ByteChunk(bytes, offset + offs, len);
	}

	public int intAt(int offs) {
		if (offs + 4 > len) {
			throw new IllegalArgumentException("Chunk " + bytes + " offs: " + offset + " len: " + len + ". Required subrange " + offs + " by " + 4);
		}
		// internal byte order - little endian
		int value =   (0xFF & bytes[offset + offs]) << 24 
					| (0xFF & bytes[offset + offs + 1]) << 16
					| (0xFF & bytes[offset + offs + 2]) << 8
					| (0xFF & bytes[offset + offs + 3]);
		return value;
	}

	public void putInt(int offs, int val) {
		if (offs + 4 > len) {
			throw new IllegalArgumentException("Out of bounds");
		}
		// internal byte order - little endian
		bytes[offset + offs] = (byte) (val >> 24);
		bytes[offset + offs + 1] = (byte) (val >> 16);
		bytes[offset + offs + 2] = (byte) (val >> 8);
		bytes[offset + offs + 3] = (byte) val;
		
	}

	public long longAt(int offs) {
		if (offs + 8 > len) {
			throw new IllegalArgumentException("Out of bounds");
		}
		// internal byte order - little endian
		long value =   (0xFFl & bytes[offset + offs]) << 56 
					 | (0xFFl & bytes[offset + offs + 1]) << 48
					 | (0xFFl & bytes[offset + offs + 2]) << 40
					 | (0xFFl & bytes[offset + offs + 3]) << 32
					 | (0xFFl & bytes[offset + offs + 4]) << 24
					 | (0xFFl & bytes[offset + offs + 5]) << 16
				   	 | (0xFFl & bytes[offset + offs + 6]) << 8
					 | (0xFFl & bytes[offset + offs + 7]);
		return value;
	}

	public void putLong(int offs, long val) {
		if (offs + 8 > len) {
			throw new IllegalArgumentException("Out of bounds");
		}
		// internal byte order - little endian
		bytes[offset + offs] = (byte) (val >> 56);
		bytes[offset + offs + 1] = (byte) (val >> 48);
		bytes[offset + offs + 2] = (byte) (val >> 40);
		bytes[offset + offs + 3] = (byte) (val >> 32);
		bytes[offset + offs + 4] = (byte) (val >> 24);
		bytes[offset + offs + 5] = (byte) (val >> 16);
		bytes[offset + offs + 6] = (byte) (val >> 8);
		bytes[offset + offs + 7] = (byte) val;
		
	}
	
	public void putBytes(ByteChunk bytes) {
		if (bytes.len > len) {
			throw new IllegalArgumentException("Out of bounds");
		}
		for(int i = 0; i != bytes.len; ++i) {
			if (this.bytes[offset + i] != 0) {
				throw new AssertionError("Chunk " + bytes + " offs: " + offset + " len: " + len + ". Dirty data for putBytes. Params " + 0 + " by " + bytes.len);
			}
		}
		System.arraycopy(bytes.bytes, bytes.offset, this.bytes, offset, bytes.len);		
	}

	public void putBytes(int offs, ByteChunk bytes) {
		if (offs + bytes.len > len) {
			throw new IllegalArgumentException("Out of bounds");
		}
		for(int i = 0; i != bytes.len; ++i) {
			if (this.bytes[offset + offs + i] != 0) {
				throw new AssertionError("Chunk " + bytes + " offs: " + offset + " len: " + len + ". Dirty data for putBytes. Params " + offs + " by " + bytes.len);
			}
		}
		System.arraycopy(bytes.bytes, bytes.offset, this.bytes, offset + offs, bytes.len);		
	}

	public boolean sameBytes(ByteChunk that) {
		if (len != that.len) {
			return false;
		}
		else {
			for(int i = 0;  i != len; ++i) {
				if (at(i) != that.at(i)) {
					return false;
				}
			}
		}
		return true;
	}

	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		buf.append('[');
		for(int i = 0; i != len; ++i) {
			byte val = at(i);
			if (i > 0 && i % 4 == 0) {
				buf.append(".");
			}
			buf.append(Integer.toHexString((val >> 4) & 0xF)).append(Integer.toHexString(val & 0xF));
			if (i > 126) {
				buf.append("...");
				break;
			}
		}
		buf.append(']');
		return buf.toString();
	}

	public void assertEmpty() {
		for(int i = 0; i != len; ++i) {
			if (bytes[offset + i] != 0) {
				throw new AssertionError("Not empty " + this.toString());
			}
		}
	}
}
