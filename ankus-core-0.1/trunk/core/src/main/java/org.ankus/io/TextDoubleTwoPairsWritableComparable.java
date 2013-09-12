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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * TextDoubleTwoPairsWritableComparable
 * @desc a WritableComparable for text and double two pairs.
 *
 * @version 0.0.1
 * @date : 2013.07.01
 * @author Suhyun Jeon
 */
public class TextDoubleTwoPairsWritableComparable implements WritableComparable<TextDoubleTwoPairsWritableComparable> {

	private Text text1;
	private double number1;
	private Text text2;
	private double number2;
		
    /** 
     * Get the value of the text1
     */ 
	public String getText1() {
		return text1.toString();
	}

    /** 
     * Get the value of the number1
     */ 
	public double getNumber1() {
		return number1;
	}

    /** 
     * Get the value of the text2
     */ 
	public String getText2() {
		return text2.toString();
	}
	
    /** 
     * Get the value of the number2
     */ 
	public double getNumber2() {
		return number2;
	}

    /** 
     * Set the value of the text1
     */ 	
	public void setText1(Text text1) {
		this.text1 = text1;
	}
	
    /** 
     * Set the value of the number2
     */ 
	public void setNumber1(double number1) {
		this.number1 = number1;
	}
	
    /** 
     * Set the value of the text2
     */ 
	public void setText2(Text text2) {
		this.text2 = text2;
	}
	
    /** 
     * Set the value of the number2
     */ 
	public void setNumber2(double number2) {
		this.number2 = number2;
	}

	public TextDoubleTwoPairsWritableComparable()	{
		text1 = new Text("");
		number1 = 0.0d;
		
		text2 = new Text("");
		number2 = 0.0d;
	}
			
	public TextDoubleTwoPairsWritableComparable(String text1, double number1, String text2, double number2) {
		this.text1 = new Text(text1);
		this.number1 = number1;		
		this.text2 = new Text(text2);
		this.number2 = number2;
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		text1.readFields(dataInput);
		number1 = dataInput.readDouble();		
		text2.readFields(dataInput);
		number2 = dataInput.readDouble();
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		text1.write(dataOutput);
		dataOutput.writeDouble(number1);		
		text2.write(dataOutput);
		dataOutput.writeDouble(number2);
	}
	
    /**
     * Returns the value of the TextDoubleTwoPairsWritableComparable
     */	
	@Override
	public String toString() {
		 return text1.toString() + "\t" + number1 + "\t" + number2;
	}

    @Override
    public int compareTo(TextDoubleTwoPairsWritableComparable textDoubleTwoPairsWritableComparable) {
        return 0;
    }
}
