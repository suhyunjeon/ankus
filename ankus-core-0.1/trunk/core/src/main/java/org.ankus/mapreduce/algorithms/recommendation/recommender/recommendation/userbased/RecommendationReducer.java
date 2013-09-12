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
package org.ankus.mapreduce.algorithms.recommendation.recommender.recommendation.userbased;

import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * RecommendationReducer
 * @desc
 *      User-based Collaborative Filtering recommendation algorithms
 *      Arrange prediction users and user's items for join
 * @version 0.0.1
 * @date : 2013.07.13
 * @author Suhyun Jeon
 */
public class RecommendationReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    private String delimiter;

	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.delimiter = configuration.get(Constants.DELIMITER);
    }  

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	Iterator<Text> iterator = values.iterator();

        List<String> predictionItemList = new ArrayList<String>();
        List<String> neighborhoodList = new ArrayList<String>();
        List<String> ratingList = new ArrayList<String>();
        List<String> userList = new ArrayList<String>();

        while (iterator.hasNext()){
            Text text = iterator.next();
            String[] columns = text.toString().split(delimiter);

            if(columns[1].equals(Constants.SIMILARITY)){
                neighborhoodList.add(columns[0]);
                userList.add(columns[2]);
                ratingList.add(columns[3]);
            }else{
                predictionItemList.add(columns[0]);
            }
        }

        double sum = 0.0d;
        int n = 0;
        for(int i=0; i<predictionItemList.size(); i++) {
            for(int k=0; k<neighborhoodList.size(); k++){
                Double ratings = Double.parseDouble(ratingList.get(k));
                sum += ratings;
                n++;
            }

            if(n != 0){
                double prediction = sum / n;
                context.write(key, new DoubleWritable(Double.parseDouble(String.format("%.3f%n", prediction))));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }

}