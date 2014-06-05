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
 * TextIntegerTwoPairsWritableComparable
 * @desc a WritableComparable for text and integer two pairs.
 *
 * @version 0.0.1
 * @date : 2013.07.01
 * @author Suhyun Jeon
 */
public class TextIntegerTwoPairsWritableComparable implements WritableComparable<TextIntegerTwoPairsWritableComparable>

{
    private Text text1;
    private int number1;
    private Text text2;
    private int number2;

    /** 
     * Get the value of the text1
     */
    public Text getText1()
    {
        return this.text1;
    }

    /** 
     * Get the value of the number1
     */
    public int getNumber1() {
        return this.number1;
    }

    /** 
     * Get the value of the text2
     */
    public Text getText2() {
        return this.text2;
    }

    /** 
     * Get the value of the number2
     */
    public int getNumber2() {
        return this.number2;
    }
   
    /** 
     * Set the value of the text1
     */
    public void setText1(Text text1) {
        this.text1 = text1;
    }
    
    /** 
     * Set the value of the number1
     */
    public void setNumber1(int number1) {
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
    public void setNumber2(int number2) {
        this.number2 = number2;
    }
 
    public TextIntegerTwoPairsWritableComparable() {
        this.text1 = new Text("");
        this.number1 = 0;

        this.text2 = new Text("");
        this.number2 = 0;
    }

    public TextIntegerTwoPairsWritableComparable(String text1, int number1, String text2, int number2) {
        this.text1 = new Text(text1);
        this.number1 = number1;

        this.text2 = new Text(text2);
        this.number2 = number2;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        this.text1.readFields(dataInput);
        this.number1 = dataInput.readInt();

        this.text2.readFields(dataInput);
        this.number2 = dataInput.readInt();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        this.text1.write(dataOutput);
        dataOutput.writeInt(this.number1);

        this.text2.write(dataOutput);
        dataOutput.writeInt(this.number2);
    }

    /**
     * Returns the value of the TextIntegerTwoPairsWritableComparable
     */
    @Override
    public String toString()
    {
        return this.text1.toString() + "\t" + this.number1 + "\t" + this.number2;
    }

    @Override
    public int compareTo(TextIntegerTwoPairsWritableComparable textIntegerTwoPairsWritableComparable) {
        return 0;
    }
}