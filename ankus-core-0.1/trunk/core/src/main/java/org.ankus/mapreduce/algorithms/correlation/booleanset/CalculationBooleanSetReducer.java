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
import java.util.*;

import org.ankus.io.TextIntegerTwoPairsWritableComparable;
import org.ankus.io.TextTwoWritableComparable;
import org.ankus.util.Constants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * CalculationBooleanSetReducer
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
public class CalculationBooleanSetReducer extends Reducer<TextTwoWritableComparable, TextIntegerTwoPairsWritableComparable, TextTwoWritableComparable, DoubleWritable> {

    private String algorithmOption;

	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	 Configuration configuration = context.getConfiguration();
         this.algorithmOption = configuration.get(Constants.ALGORITHM_OPTION);
    }

	@Override
    protected void reduce(TextTwoWritableComparable key, Iterable<TextIntegerTwoPairsWritableComparable> values, Context context) throws IOException, InterruptedException {

        if(algorithmOption.equals(Constants.HAMMING_DISTACNE_FOR_BOOLEAN)){
            int hammingDistance = 0;

            Map<String, Integer> itemID1Map = new HashMap<String, Integer>();
            Map<String, Integer> itemID2Map = new HashMap<String, Integer>();

            for(TextIntegerTwoPairsWritableComparable textIntegerPairsWritable : values) {
                itemID1Map.put(textIntegerPairsWritable.getText1().toString(), textIntegerPairsWritable.getNumber1());
                itemID2Map.put(textIntegerPairsWritable.getText2().toString(), textIntegerPairsWritable.getNumber2());
            }
            
            char[] item1CharArray = itemID1Map.toString().toCharArray();
            char[] item2CharArray = itemID2Map.toString().toCharArray();

            int item1CharArrayLength = item1CharArray.length;
            int item2CharArrayLength = item2CharArray.length;

            if (itemID1Map.containsValue(itemID2Map)) hammingDistance = 0;
            if (item1CharArrayLength != item2CharArrayLength) {
                hammingDistance = -1;
            }else{
                for (int i = 0; i < item1CharArrayLength; ++i){
                    if (itemID1Map.toString().charAt(i) == itemID2Map.toString().charAt(i)){
                        hammingDistance += 0;
                    }else if (itemID1Map.toString().charAt(i) != itemID2Map.toString().charAt(i)){
                        ++hammingDistance;
                    }
                }
            }
            context.write(key, new DoubleWritable(hammingDistance));

        }else if(algorithmOption.equals(Constants.DICE_COEFFICIENT)){
            double diceCoefficient = 0.0d;
            int size1 = 0;
            int size2 = 0;

            Map<String, Integer> itemID1Map = new HashMap<String, Integer>();
            Map<String, Integer> itemID2Map = new HashMap<String, Integer>();

            for(TextIntegerTwoPairsWritableComparable textIntegerPairsWritable : values) {
                itemID1Map.put(textIntegerPairsWritable.getText1().toString(), textIntegerPairsWritable.getNumber1());
                itemID2Map.put(textIntegerPairsWritable.getText2().toString(), textIntegerPairsWritable.getNumber2());

                size1 += textIntegerPairsWritable.getNumber1();
                size2 += textIntegerPairsWritable.getNumber2();
            }

            // Find the intersection, and get the number of elements in that set.
            Collection<String> intersection = CollectionUtils.intersection(itemID1Map.entrySet(), itemID2Map.entrySet());

            diceCoefficient = (2.0 * (float)intersection.size()) / ((float)(size1 + size2));
            context.write(key, new DoubleWritable(Double.parseDouble(String.format("%.3f%n", diceCoefficient))));

        }else if(algorithmOption.equals(Constants.JACCARD_COEFFICIENT)){
        	double jaccardCoefficient = 0.0d;
            int unionSize = 0;

        	Map<String, Integer> itemID1Map = new HashMap<String, Integer>();
        	Map<String, Integer> itemID2Map = new HashMap<String, Integer>();
        	
        	for(TextIntegerTwoPairsWritableComparable textIntegerPairsWritable : values) {
                itemID1Map.put(textIntegerPairsWritable.getText1().toString(), textIntegerPairsWritable.getNumber1());
                itemID2Map.put(textIntegerPairsWritable.getText2().toString(), textIntegerPairsWritable.getNumber2());

                if((textIntegerPairsWritable.getNumber1() + textIntegerPairsWritable.getNumber2()) >= 1){
                    unionSize += 1;
                }
        	}
        	
        	Collection<String> intersection = CollectionUtils.intersection(itemID1Map.entrySet(), itemID2Map.entrySet());

            jaccardCoefficient = (float)intersection.size() / (float)unionSize;
            context.write(key, new DoubleWritable(Double.parseDouble(String.format("%.3f%n", jaccardCoefficient))));
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}