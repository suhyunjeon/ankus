/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ankus.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * IntegerTwoWritableComparable
 * @desc a WritableComparable for integer pair.
 *
 * @version 0.0.1
 * @date : 2013.07.01
 * @author Suhyun Jeon
 */
public class IntegerTwoWritableComparable implements WritableComparable<IntegerTwoWritableComparable> {

	private Integer number1;
	private Integer number2;
	
    /** 
     * Get the value of the number1
     */ 
	public Integer getNumber1() {
		return number1;
	}

    /** 
     * Get the value of the number2
     */ 	
	public Integer getNumber2() {
		return number2;
	}
	
    /** 
     * Set the value of the number1
     */ 
	public void setNumber1(Integer number1) {
		this.number1 = number1;
	}
	
    /** 
     * Set the value of the number2
     */ 
	public void setNumber2(Integer number2) {
		this.number2 = number2;
	}

	public void setTextPairsWritable(Integer number1, Integer number2) {
		this.number1 = number1;
		this.number2 = number2;
	}	
	
	public IntegerTwoWritableComparable() {
		setTextPairsWritable(new Integer(0), new Integer(0));
	}
	
	public IntegerTwoWritableComparable(Integer number1, Integer number2) {
		setTextPairsWritable(new Integer(number1), new Integer(number2));
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		number1 = dataInput.readInt();
		number2 = dataInput.readInt();
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(number1);
		dataOutput.writeInt(number2);
	}
	
    /**
     * Returns the value of the IntegerTwoWritableComparable
     */	
	@Override
	public String toString() {
		 return number1 + "\t" + number2;
	}

    @Override
    public int compareTo(IntegerTwoWritableComparable integerTwoWritableComparable) {
        return 0;
    }
}
