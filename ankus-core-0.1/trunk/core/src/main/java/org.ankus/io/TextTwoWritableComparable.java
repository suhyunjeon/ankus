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
 * TextTwoWritableComparable
 * @desc a WritableComparable for two texts.
 *
 * @version 0.0.1
 * @date : 2013.07.01
 * @author Suhyun Jeon
 */
public class TextTwoWritableComparable implements WritableComparable<TextTwoWritableComparable> {

	private Text text1;
	private Text text2; 
	
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
     * Set the value of the text1
     */   
	public void setText1(Text text1) {
		this.text1 = text1;
	}

    /** 
     * Set the value of the text2
     */   
	public void setText2(Text text2) {
		this.text2 = text2;
	}

	public void setTextTwoWritable(Text text1, Text text2) {
		this.text1 = text1;
		this.text2 = text2;
	}

    public TextTwoWritableComparable() {
		setTextTwoWritable(new Text(""), new Text(""));
	}
	
	public TextTwoWritableComparable(String text1, String text2) {
		setTextTwoWritable(new Text(text1), new Text(text2));
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		text1.readFields(dataInput);
		text2.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		text1.write(dataOutput);
		text2.write(dataOutput);
	}
	
	@Override
	public String toString() {
		 return text1.toString() + "\t" + text2.toString();
	}

    @Override
    public int compareTo(TextTwoWritableComparable textTwoWritableComparable) {
        return 0;
    }
}
