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
package org.ankus.mapreduce.algorithms.correlation.booleanset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.ankus.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * BooleanSetReducer
 * @desc
 *      Here's an version of the similarity coefficient and distance calculation.
 *      1. Dice coefficient 2. Jaccard coefficient 3. Hamming distance
 *
 * Example dataset
 * ------------------------
 * 1    0   1   1   1   0
 * 0    0   0   0   1   1
 * 1    0   1   0   1   0
 *
 * @return The is between the two input VECTOR boolean dataset..
 * 		   Returns 1 if one 0 or both of the booleans are not {@code 0 or 1}.
 *
 * @version 0.0.1
 * @date : 2013.07.10
 * @author Suhyun Jeon
*/
public class BooleanSetReducer extends Reducer<Text, TextIntegerPairWritableComparable, TextTwoWritableComparable, TextIntegerTwoPairsWritableComparable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    protected void reduce(Text key, Iterable<TextIntegerPairWritableComparable> values, Context context) throws IOException, InterruptedException {
       
        Iterator<TextIntegerPairWritableComparable> iterator = values.iterator();
        List<String> uniqueKeyList = new ArrayList<String>();
        List<Integer> valueList = new ArrayList<Integer>();

        while(iterator.hasNext()){
        	TextIntegerPairWritableComparable textIntegerPairWritableComparable = iterator.next();

        	uniqueKeyList.add(textIntegerPairWritableComparable.getText().toString());
        	valueList.add(textIntegerPairWritableComparable.getNumber());
        }

        TextTwoWritableComparable textTwoWritableComparable = null;
        TextIntegerTwoPairsWritableComparable textIntegerPairsWritableComparable = null;

        for(int i=0; i<uniqueKeyList.size(); i++) {
        	for(int j=i+1; j<uniqueKeyList.size(); j++) {
                if(uniqueKeyList.get(i).compareTo(uniqueKeyList.get(j)) > 0){
                    textTwoWritableComparable = new TextTwoWritableComparable(uniqueKeyList.get(j), uniqueKeyList.get(i));
                    textIntegerPairsWritableComparable = new TextIntegerTwoPairsWritableComparable(key.toString(), valueList.get(j), key.toString(), valueList.get(i));
                }else{
                    textTwoWritableComparable = new TextTwoWritableComparable(uniqueKeyList.get(i), uniqueKeyList.get(j));
                    textIntegerPairsWritableComparable = new TextIntegerTwoPairsWritableComparable(key.toString(), valueList.get(i), key.toString(), valueList.get(j));

                }
        		context.write(textTwoWritableComparable, textIntegerPairsWritableComparable);
        	}
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}