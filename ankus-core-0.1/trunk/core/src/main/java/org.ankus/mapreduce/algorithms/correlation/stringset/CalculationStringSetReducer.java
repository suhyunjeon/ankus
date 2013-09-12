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
package org.ankus.mapreduce.algorithms.correlation.stringset;

import java.io.IOException;

import org.ankus.io.TextFourWritableComparable;
import org.ankus.io.TextTwoWritableComparable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * CalculationStringSetReducer
 * @desc
 *      Here's an version of the similarity coefficient and distance calculation.
 *      1. Hamming distance 2. Edit distance
 * @version 0.0.1
 * @date : 2013.07.11
 * @author Suhyun Jeon
*/
public class CalculationStringSetReducer extends Reducer<TextTwoWritableComparable, TextFourWritableComparable, TextTwoWritableComparable, DoubleWritable> {

	private String algorithmOption;

	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		this.algorithmOption = configuration.get(Constants.ALGORITHM_OPTION);
    }

    @Override
    protected void reduce(TextTwoWritableComparable key, Iterable<TextFourWritableComparable> values, Context context) throws IOException, InterruptedException {
       
    	if(algorithmOption.equals(Constants.EDIT_DISTANCE)){
    		
    		int editDistance = 0;
        	String wordUserID1 = null;
        	String wordUserID2 = null;
        	int[][] matrix = null;

        	for(TextFourWritableComparable textFourWritableComparable : values) {
        		wordUserID1 = textFourWritableComparable.getText2().toString();
        		wordUserID2 = textFourWritableComparable.getText4().toString();
        	
        		char[] word1CharArray = wordUserID1.toCharArray();
        		char[] word2CharArray = wordUserID2.toCharArray();
        		
        		int word1CharArrayLength = word1CharArray.length;
        		int word2CharArrayLength = word2CharArray.length;
        		
        		// Degenerate cases
        		if(wordUserID1.equals(wordUserID2)){
                    editDistance =  0;
                    context.write(key, new DoubleWritable(editDistance));
                }
        		if(word1CharArrayLength == 0){
                    editDistance =  word2CharArrayLength;
                    context.write(key, new DoubleWritable(editDistance));
                }
        		if(word2CharArrayLength == 0){
                    editDistance =  word1CharArrayLength;
                    context.write(key, new DoubleWritable(editDistance));
                }

        		// Create two work vectors of integer distance
        		matrix = new int[word1CharArrayLength + 1][word2CharArrayLength + 1];
        		
        		for(int i=0; i<matrix.length; i++){
        			for(int j=0; j<matrix[i].length; j++){
        				// The edit distance between an empty string and the prefixes of word1
        				if(i == 0)
        					matrix[i][j] = j;
        				// The edit distance between an empty string and the prefixes of word2
                        else if(j == 0)
                        	matrix[i][j] = i;
                        else {
                        	if(wordUserID1.charAt(i-1) == wordUserID2.charAt(j-1)){
                        		matrix[i][j] = matrix[i-1][j-1];
                        	}else{
                        		// Min of insertion, deletion, replacement
        						matrix[i][j] = 1 + Math.min(Math.min(matrix[i-1][j-1], matrix[i-1][j]), matrix[i][j-1]);
                        	}
        				}
        			}
        		}
        	}
      
        	editDistance = matrix[wordUserID1.length()][wordUserID2.length()];
       		context.write(key, new DoubleWritable(editDistance));
       		
    	}else if(algorithmOption.equals(Constants.HAMMING_DISTANCE_FOR_STRING)){
    		
    		String wordUserID1 = null;
        	String wordUserID2 = null;
        	int hammingDistance = 0;

        	for(TextFourWritableComparable textFourWritableComparable : values) {
        		wordUserID1 = textFourWritableComparable.getText2().toString();
        		wordUserID2 = textFourWritableComparable.getText4().toString();
        	
        		char[] word1CharArray = wordUserID1.toCharArray();
        		char[] word2CharArray = wordUserID2.toCharArray();
        		
        		int word1CharArrayLength = word1CharArray.length;
        		int word2CharArrayLength = word2CharArray.length;
        	
        		if(wordUserID1.equals(wordUserID2)) hammingDistance =  0;
        		
        		// Input words should be of equal length
        		if(word1CharArrayLength != word2CharArrayLength){
        			hammingDistance =  -1;
        		}else{
        			// Both are of the same length
            		for(int i=0; i<word1CharArrayLength; i++){
                    	if(wordUserID1.charAt(i) == wordUserID2.charAt(i)){
                    		hammingDistance += 0;
                    	}else if(wordUserID1.charAt(i) != wordUserID2.charAt(i)){
                    		hammingDistance += 1;
                    	}
            		}
        		}
        	}
       		context.write(key, new DoubleWritable(hammingDistance));
    	}
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}