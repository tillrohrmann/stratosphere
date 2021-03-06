/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

// --------------------------------------------------------------
//  THIS IS A GENERATED SOURCE FILE. DO NOT EDIT!
//  GENERATED FROM eu.stratosphere.api.java.tuple.TupleGenerator.
// --------------------------------------------------------------


package eu.stratosphere.api.java.tuple;

import eu.stratosphere.util.StringUtils;

/**
 * A tuple with 9 fields. Tuples are strongly typed; each field may be of a separate type.
 * The fields of the tuple can be accessed directly as public fields (f0, f1, ...) or via their position
 * through the {@link #getField(int)} method. The tuple field positions start at zero.
 * <p>
 * Tuples are mutable types, meaning that their fields can be re-assigned. This allows functions that work
 * with Tuples to reuse objects in order to reduce pressure on the garbage collector.
 *
 * @see Tuple
 *
 * @param <T0> The type of field 0
 * @param <T1> The type of field 1
 * @param <T2> The type of field 2
 * @param <T3> The type of field 3
 * @param <T4> The type of field 4
 * @param <T5> The type of field 5
 * @param <T6> The type of field 6
 * @param <T7> The type of field 7
 * @param <T8> The type of field 8
 */
@SuppressWarnings({"restriction"})
public class Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8> extends Tuple {

	private static final long serialVersionUID = 1L;

	/** Field 0 of the tuple. */
	public T0 f0;
	/** Field 1 of the tuple. */
	public T1 f1;
	/** Field 2 of the tuple. */
	public T2 f2;
	/** Field 3 of the tuple. */
	public T3 f3;
	/** Field 4 of the tuple. */
	public T4 f4;
	/** Field 5 of the tuple. */
	public T5 f5;
	/** Field 6 of the tuple. */
	public T6 f6;
	/** Field 7 of the tuple. */
	public T7 f7;
	/** Field 8 of the tuple. */
	public T8 f8;

	/**
	 * Creates a new tuple where all fields are null.
	 */
	public Tuple9() {}

	/**
	 * Creates a new tuple and assigns the given values to the tuple's fields.
	 *
	 * @param value0 The value for field 0
	 * @param value1 The value for field 1
	 * @param value2 The value for field 2
	 * @param value3 The value for field 3
	 * @param value4 The value for field 4
	 * @param value5 The value for field 5
	 * @param value6 The value for field 6
	 * @param value7 The value for field 7
	 * @param value8 The value for field 8
	 */
	public Tuple9(T0 value0, T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8) {
		this.f0 = value0;
		this.f1 = value1;
		this.f2 = value2;
		this.f3 = value3;
		this.f4 = value4;
		this.f5 = value5;
		this.f6 = value6;
		this.f7 = value7;
		this.f8 = value8;
	}

	@Override
	public int getArity() { return 9; }

	@Override
	@SuppressWarnings("unchecked")
	public <T> T getField(int pos) {
		switch(pos) {
			case 0: return (T) this.f0;
			case 1: return (T) this.f1;
			case 2: return (T) this.f2;
			case 3: return (T) this.f3;
			case 4: return (T) this.f4;
			case 5: return (T) this.f5;
			case 6: return (T) this.f6;
			case 7: return (T) this.f7;
			case 8: return (T) this.f8;
			default: throw new IndexOutOfBoundsException(String.valueOf(pos));
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> void setField(T value, int pos) {
		switch(pos) {
			case 0:
				this.f0 = (T0) value;
				break;
			case 1:
				this.f1 = (T1) value;
				break;
			case 2:
				this.f2 = (T2) value;
				break;
			case 3:
				this.f3 = (T3) value;
				break;
			case 4:
				this.f4 = (T4) value;
				break;
			case 5:
				this.f5 = (T5) value;
				break;
			case 6:
				this.f6 = (T6) value;
				break;
			case 7:
				this.f7 = (T7) value;
				break;
			case 8:
				this.f8 = (T8) value;
				break;
			default: throw new IndexOutOfBoundsException(String.valueOf(pos));
		}
	}

	/**
	 * Sets new values to all fields of the tuple.
	 *
	 * @param value0 The value for field 0
	 * @param value1 The value for field 1
	 * @param value2 The value for field 2
	 * @param value3 The value for field 3
	 * @param value4 The value for field 4
	 * @param value5 The value for field 5
	 * @param value6 The value for field 6
	 * @param value7 The value for field 7
	 * @param value8 The value for field 8
	 */
	public void setFields(T0 value0, T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8) {
		this.f0 = value0;
		this.f1 = value1;
		this.f2 = value2;
		this.f3 = value3;
		this.f4 = value4;
		this.f5 = value5;
		this.f6 = value6;
		this.f7 = value7;
		this.f8 = value8;
	}


	// -------------------------------------------------------------------------------------------------
	// standard utilities
	// -------------------------------------------------------------------------------------------------

	/**
	 * Creates a string representation of the tuple in the form
	 * (f0, f1, f2, f3, f4, f5, f6, f7, f8),
	 * where the individual fields are the value returned by calling {@link Object#toString} on that field.
	 * @return The string representation of the tuple.
	 */
	@Override
	public String toString() {
		return "(" + StringUtils.arrayAwareToString(this.f0)
			+ ", " + StringUtils.arrayAwareToString(this.f1)
			+ ", " + StringUtils.arrayAwareToString(this.f2)
			+ ", " + StringUtils.arrayAwareToString(this.f3)
			+ ", " + StringUtils.arrayAwareToString(this.f4)
			+ ", " + StringUtils.arrayAwareToString(this.f5)
			+ ", " + StringUtils.arrayAwareToString(this.f6)
			+ ", " + StringUtils.arrayAwareToString(this.f7)
			+ ", " + StringUtils.arrayAwareToString(this.f8)
			+ ")";
	}

	// -------------------------------------------------------------------------------------------------
	// unsafe fast field access
	// -------------------------------------------------------------------------------------------------

	@SuppressWarnings({ "unchecked"})
	public <T> T getFieldFast(int pos) {
		return (T) UNSAFE.getObject(this, offsets[pos]);
	}

	private static final sun.misc.Unsafe UNSAFE = eu.stratosphere.core.memory.MemoryUtils.UNSAFE;

	private static final long[] offsets = new long[9];

	static {
		try {
			offsets[0] = UNSAFE.objectFieldOffset(Tuple9.class.getDeclaredField("f0"));
			offsets[1] = UNSAFE.objectFieldOffset(Tuple9.class.getDeclaredField("f1"));
			offsets[2] = UNSAFE.objectFieldOffset(Tuple9.class.getDeclaredField("f2"));
			offsets[3] = UNSAFE.objectFieldOffset(Tuple9.class.getDeclaredField("f3"));
			offsets[4] = UNSAFE.objectFieldOffset(Tuple9.class.getDeclaredField("f4"));
			offsets[5] = UNSAFE.objectFieldOffset(Tuple9.class.getDeclaredField("f5"));
			offsets[6] = UNSAFE.objectFieldOffset(Tuple9.class.getDeclaredField("f6"));
			offsets[7] = UNSAFE.objectFieldOffset(Tuple9.class.getDeclaredField("f7"));
			offsets[8] = UNSAFE.objectFieldOffset(Tuple9.class.getDeclaredField("f8"));
		} catch (Throwable t) {
			throw new RuntimeException("Could not initialize fast field accesses for tuple data type.");
		}
	}
}
