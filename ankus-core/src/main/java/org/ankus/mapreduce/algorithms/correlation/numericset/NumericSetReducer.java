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
package org.ankus.mapreduce.algorithms.correlation.numericset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.ankus.io.TextDoublePairWritableComparable;
import org.ankus.io.TextDoubleTwoPairsWritableComparable;
import org.ankus.io.TextTwoWritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * NumericSetReducer
 * @desc
 *      Here's an version of the similarity coefficient and distance calculation.
 *      1. Cosine coefficient 2. Pearson coefficient 3. Tanimoto coefficient
 *      4. Manhattan distance 5. Uclidean distance
 *       Required data items : allow only numeric values
 * @version 0.0.1
 * @date : 2013.07.11
 * @author Suhyun Jeon
*/
public class NumericSetReducer extends Reducer<Text, TextDoublePairWritableComparable, TextTwoWritableComparable, TextDoubleTwoPairsWritableComparable> {
      
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    protected void reduce(Text key, Iterable<TextDoublePairWritableComparable> values, Context context) throws IOException, InterruptedException {
    	
    	Iterator<TextDoublePairWritableComparable> iterator = values.iterator();
    	List<String> uniqueKeyList = new ArrayList<String>();
    	List<Double> valueList = new ArrayList<Double>();
              
        while (iterator.hasNext()) {
            TextDoublePairWritableComparable textDoublePairWritableComparable = iterator.next();

        	uniqueKeyList.add(textDoublePairWritableComparable.getText().toString());
        	valueList.add(textDoublePairWritableComparable.getNumber());
        }

        TextTwoWritableComparable textTwoWritableComparable = null;
        TextDoubleTwoPairsWritableComparable textDoubleTwoPairsWritableComparable = null;

        for(int i=0; i<valueList.size(); i++) {
        	for(int j=i+1; j<valueList.size(); j++) {
                if(uniqueKeyList.get(i).compareTo(uniqueKeyList.get(j)) > 0){
                    textTwoWritableComparable = new TextTwoWritableComparable(uniqueKeyList.get(j), uniqueKeyList.get(i));
                    textDoubleTwoPairsWritableComparable = new TextDoubleTwoPairsWritableComparable(key.toString(), valueList.get(j), key.toString(), valueList.get(i));
                }else{
                    textTwoWritableComparable = new TextTwoWritableComparable(uniqueKeyList.get(i), uniqueKeyList.get(j));
                    textDoubleTwoPairsWritableComparable = new TextDoubleTwoPairsWritableComparable(key.toString(), valueList.get(i), key.toString(), valueList.get(j));
                }
                context.write(textTwoWritableComparable, textDoubleTwoPairsWritableComparable);
        	}
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }

}