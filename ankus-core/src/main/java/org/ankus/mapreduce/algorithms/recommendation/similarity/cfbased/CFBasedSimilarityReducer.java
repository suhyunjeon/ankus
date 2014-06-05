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
package org.ankus.mapreduce.algorithms.recommendation.similarity.cfbased;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.ankus.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * CFBasedSimilarityReducer
 * @desc
 *      User-based Collaborative Filtering recommendation algorithms
 *      1. Cosine coefficient 2. Pearson coefficient
 *      Required data set : [userID, itemID, rating]
 * @version 0.0.1
 * @date : 2013.07.20
 * @author Suhyun Jeon
 */
public class CFBasedSimilarityReducer extends Reducer<Text, TextDoublePairWritableComparable, TextTwoWritableComparable, TextDoubleTwoPairsWritableComparable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    protected void reduce(Text key, Iterable<TextDoublePairWritableComparable> values, Context context) throws IOException, InterruptedException {

        Iterator<TextDoublePairWritableComparable> iterator = values.iterator();
        List<String> keyIDList = new ArrayList<String>();
        List<Double> ratingList = new ArrayList<Double>();

        while (iterator.hasNext()) {
            TextDoublePairWritableComparable textTwoWritableComparable = iterator.next();

            keyIDList.add(textTwoWritableComparable.getText());
            ratingList.add(textTwoWritableComparable.getNumber());
        }

        TextTwoWritableComparable textTwoWritableComparable = null;
        TextDoubleTwoPairsWritableComparable textDoubleTwoPairsWritableComparable = null;

        for(int i=0; i<keyIDList.size(); i++) {
            for(int j=i+1; j<keyIDList.size(); j++) {
                if(keyIDList.get(i).compareTo(keyIDList.get(j)) > 0){
                    textTwoWritableComparable = new TextTwoWritableComparable(keyIDList.get(j), keyIDList.get(i));
                    textDoubleTwoPairsWritableComparable = new TextDoubleTwoPairsWritableComparable(key.toString(), ratingList.get(j), key.toString(), ratingList.get(i));
                }else{
                    textTwoWritableComparable = new TextTwoWritableComparable(keyIDList.get(i), keyIDList.get(j));
                    textDoubleTwoPairsWritableComparable = new TextDoubleTwoPairsWritableComparable(key.toString(), ratingList.get(i), key.toString(), ratingList.get(j));
                }
                context.write(textTwoWritableComparable, textDoubleTwoPairsWritableComparable);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}