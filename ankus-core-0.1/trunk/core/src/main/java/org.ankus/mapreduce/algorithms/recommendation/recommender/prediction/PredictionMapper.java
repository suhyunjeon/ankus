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
package org.ankus.mapreduce.algorithms.recommendation.recommender.prediction;

import org.ankus.io.TextTwoWritableComparable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * PredictionMapper
 * @desc
 *      User-based Collaborative Filtering recommendation algorithms
 *      Arrange prediction items for n users
 * @version 0.0.1
 * @date : 2013.07.13
 * @author Suhyun Jeon
 */
public class PredictionMapper extends Mapper<LongWritable, Text, Text, TextTwoWritableComparable> {

    private String delimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.delimiter = configuration.get(Constants.DELIMITER);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String row = value.toString();
        String[] columns = row.split(delimiter);

        String userID = columns[0];
        String item = columns[1];
        String rating = columns[2];

        TextTwoWritableComparable textTwoWritableComparable = new TextTwoWritableComparable(item, rating);
        context.write(new Text(userID), textTwoWritableComparable);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
