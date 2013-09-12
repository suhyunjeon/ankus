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
 * TextFourWritableComparable
 * @desc a WritableComparable for four text.
 *
 * @version 0.0.1
 * @date : 2013.07.01
 * @author Suhyun Jeon
 */
public class TextFourWritableComparable implements WritableComparable<TextFourWritableComparable> {

	private Text text1;
	private Text text2;
	private Text text3;
	private Text text4;
	
    /** 
     * Get the value of the text1
     */ 	
	public Text getText1() {
		return text1;
	}

    /** 
     * Get the value of the text2
     */ 
	public Text getText2() {
		return text2;
	}

    /** 
     * Get the value of the text3
     */ 
	public Text getText3() {
		return text3;
	}

    /** 
     * Get the value of the text4
     */ 
	public Text getText4() {
		return text4;
	}

	public void setTextFourWritableComparable(Text text1, Text text2, Text text3, Text text4) {
		this.text1 = text1;
		this.text2 = text2;
		this.text3 = text3;
		this.text4 = text4;
	}	
	
	public void setText1(Text text1) {
		this.text1 = text1;
	}

	public void setText2(Text text2) {
		this.text2 = text2;
	}

	public void setText3(Text text3) {
		this.text3 = text3;
	}

	public void setText4(Text text4) {
		this.text4 = text4;
	}

	public TextFourWritableComparable() {
		setTextFourWritableComparable(new Text(""), new Text(""), new Text(""), new Text(""));
	}
	
	public TextFourWritableComparable(String text1, String text2, String text3, String text4) {
		setTextFourWritableComparable(new Text(text1), new Text(text2), new Text(text3), new Text(text4));
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		text1.readFields(dataInput);
		text2.readFields(dataInput);
		text3.readFields(dataInput);
		text4.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		text1.write(dataOutput);
		text2.write(dataOutput);
		text3.write(dataOutput);
		text4.write(dataOutput);
	}
	
    /**
     * Returns the value of the TextFourWritableComparable
     */	
	@Override
	public String toString() {
		 return text1.toString() + "\t" + text2.toString() + "\t" + text4.toString();
	}

    @Override
    public int compareTo(TextFourWritableComparable textFourWritableComparable) {
        return 0;
    }
}
