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
package org.ankus.mapreduce.algorithms.recommendation.similarity.contentbased;

import org.ankus.io.TextFourWritableComparable;
import org.ankus.io.TextTwoWritableComparable;
import org.ankus.util.Constants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

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
 * @version 0.1
 * @date : 2013.07.10
 * @author Suhyun Jeon
*/
public class CalculationContentBasedSimilarityReducer extends Reducer<TextTwoWritableComparable, TextFourWritableComparable, TextTwoWritableComparable, DoubleWritable> {

    private String algorithmOption;

	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	 Configuration configuration = context.getConfiguration();
         this.algorithmOption = configuration.get(Constants.ALGORITHM_OPTION);
    }

	@Override
    protected void reduce(TextTwoWritableComparable key, Iterable<TextFourWritableComparable> values, Context context) throws IOException, InterruptedException {

        if(algorithmOption.equals(Constants.JACCARD_COEFFICIENT)){

            Collection<String> firstList = new ArrayList<String>();
            Collection<String> secondList = new ArrayList<String>();

            for(TextFourWritableComparable textFourWritableComparable : values) {
                firstList.add(textFourWritableComparable.getText2().toString().trim());
                secondList.add(textFourWritableComparable.getText4().toString().trim());
            }

            // Remove duplicate items
            List<String> firstItems = new ArrayList<String>(new HashSet<String>(firstList));
            List<String> secondItems = new ArrayList<String>(new HashSet<String>(secondList));

            // Union of list
            Collection union = CollectionUtils.union(firstItems, secondItems);
            // Intersection of list
            Collection intersection = CollectionUtils.intersection(firstItems, secondItems);

            double jaccardCoefficient = (float)intersection.size() / (float)union.size();
            context.write(key, new DoubleWritable(Double.parseDouble(String.format("%.3f%n", jaccardCoefficient))));

        }else if(algorithmOption.equals(Constants.HAMMING_DISTACNE)){
            int hammingDistance = 0;

            Collection<String> firstList = new ArrayList<String>();
            Collection<String> secondList = new ArrayList<String>();

            for(TextFourWritableComparable textFourWritableComparable : values) {
                firstList.add(textFourWritableComparable.getText2().toString().trim());
                secondList.add(textFourWritableComparable.getText4().toString().trim());
            }

            // Remove duplicate items
            List<String> firstItemList = new ArrayList<String>(new HashSet<String>(firstList));
            List<String> secondItemList = new ArrayList<String>(new HashSet<String>(secondList));

            char[] item1CharArray = firstItemList.toString().toCharArray();
            char[] item2CharArray = secondItemList.toString().toCharArray();

            int item1CharArrayLength = item1CharArray.length;
            int item2CharArrayLength = item2CharArray.length;

            if(firstItemList.contains(secondItemList)){
                hammingDistance = 0;
            }

            if (item1CharArrayLength != item2CharArrayLength) {
                hammingDistance = -1;
            }else{
                for (int i = 0; i < firstItemList.size(); ++i){
                    String first = firstItemList.get(i);
                    String second = secondItemList.get(i);

                    for(int k=0; k<first.length(); k++){
                        if (first.charAt(i) == second.charAt(i)){
                            hammingDistance += 0;
                        }else if (first.charAt(i) != second.charAt(i)){
                            hammingDistance++;
                        }
                    }
                }
            }

            context.write(key, new DoubleWritable(hammingDistance));


        }else if(algorithmOption.equals(Constants.DICE_COEFFICIENT)){
            Collection<String> firstList = new ArrayList<String>();
            Collection<String> secondList = new ArrayList<String>();

            for(TextFourWritableComparable textFourWritableComparable : values) {
                firstList.add(textFourWritableComparable.getText2().toString().trim());
                secondList.add(textFourWritableComparable.getText4().toString().trim());
            }

            // Remove duplicate items
            List<String> firstItemList = new ArrayList<String>(new HashSet<String>(firstList));
            List<String> secondItemList = new ArrayList<String>(new HashSet<String>(secondList));

            char[] item1CharArray = firstItemList.toString().toCharArray();
            char[] item2CharArray = secondItemList.toString().toCharArray();

            int item1CharArrayLength = item1CharArray.length;
            int item2CharArrayLength = item2CharArray.length;

            // Intersection of list
            Collection intersection = CollectionUtils.intersection(firstItemList, secondItemList);

            double diceCoefficient = (2.0 * (float)intersection.size()) / ((float)(item1CharArrayLength + item2CharArrayLength));
            context.write(key, new DoubleWritable(Double.parseDouble(String.format("%.3f%n", diceCoefficient))));

        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}