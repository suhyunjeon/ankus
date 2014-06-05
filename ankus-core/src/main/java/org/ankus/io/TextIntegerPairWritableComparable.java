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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
 
/**
 * TextIntegerPairWritableComparable
 * @desc a WritableComparable for text and integer two pairs.
 *
 * @version 0.0.1
 * @date : 2013.07.01
 * @author Suhyun Jeon
 */
public class TextIntegerPairWritableComparable implements WritableComparable<TextIntegerPairWritableComparable>
{
    private Integer number;
    private Text text;

    /** 
     * Get the value of the number
     */
    public Integer getNumber()
    {
        return number;
    }

    /** 
     * Get the value of the number
     */
    public Text getText() {
        return text;
    }
    
    /** 
     * Set the value of the text
     */   
    public void setText(Text text) {
        this.text = text;
    }
    
    /** 
     * Set the value of the number
     */
    public void setNumber(Integer number) {
        this.number = number;
    }

    public void setTextIntegerPairWritableComparable(Text text, Integer number) {
        this.text = text;
        this.number = number;
    }

    public TextIntegerPairWritableComparable() {
        setTextIntegerPairWritableComparable(new Text(""), new Integer(0));
    }

    public TextIntegerPairWritableComparable(String text, Integer number) {
        setTextIntegerPairWritableComparable(new Text(text), new Integer(number.intValue()));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        this.text.readFields(dataInput);
        this.number = Integer.valueOf(dataInput.readInt());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        this.text.write(dataOutput);
        dataOutput.writeInt(this.number.intValue());
    }

    /**
     * Returns the value of the TextIntegerPairWritableComparable
     */
    @Override
    public String toString()
    {
        return text.toString() + "\t" + number;
    }

    @Override
    public int compareTo(TextIntegerPairWritableComparable textIntegerPairWritableComparable) {
        return 0;
    }
}