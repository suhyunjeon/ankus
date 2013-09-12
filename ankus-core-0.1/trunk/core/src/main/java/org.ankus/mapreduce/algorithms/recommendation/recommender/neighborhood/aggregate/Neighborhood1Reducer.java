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
package org.ankus.mapreduce.algorithms.recommendation.recommender.neighborhood.aggregate;

import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Neighborhood1Reducer
 * @desc
 *      User-based Collaborative Filtering recommendation algorithms
 *      Arrange data set from Movielens type
 *      ---------------------------------------------
 *      userID  movieID  rating     timestamp
 *      ---------------------------------------------
 *      196     242	        3	    881250949
 *      186	    302	        3	    891717742
 *      22	    377	        1	    878887116
 *      244	    51	        2	    880606923
 *      166	    346	        1	    88639759
 *      ---------------------------------------------
 * @version 0.0.1
 * @date : 2013.07.13
 * @author Suhyun Jeon
 */
public class Neighborhood1Reducer extends Reducer<Text, Text, Text, Text> {

    private String delimiter;

	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.delimiter = configuration.get(Constants.DELIMITER);
    }  

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	Iterator<Text> iterator = values.iterator();

        while (iterator.hasNext()){
            Text text = iterator.next();
            String[] columns = text.toString().split(delimiter);

            context.write(key, new Text(columns[0]));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }

}