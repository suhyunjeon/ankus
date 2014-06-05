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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * CalculationBooleanSetMapper
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
 * @return The is between the two input VECTOR boolean data set.
 * 		   Returns 1 if one 0 or both of the boolean are not {@code 0 or 1}.
 *
 * @version 0.1
 * @date : 2013.11.05
 * @author Suhyun Jeon
*/
public class CalculationContentBasedSimilarityMapper extends Mapper<LongWritable, Text, TextTwoWritableComparable, TextFourWritableComparable> {

    private String delimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.delimiter = configuration.get(Constants.DELIMITER);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	String row = value.toString();

        //TODO 얘는 delimiter를 :: 가 아닌 다른걸로 해야 함.
//        String[] columns = row.split(delimiter);
        String[] columns = row.split("\t");

        String id1 = columns[0];
        String id2 = columns[1];
        String item = columns[2];
        String value1 = columns[3];
        String value2 = columns[4];

        TextTwoWritableComparable textTwoWritableComparable = new TextTwoWritableComparable(id1, id2 + "\t" + item);
        TextFourWritableComparable textFourWritableComparable = new TextFourWritableComparable(item, value1, item, value2);
        context.write(textTwoWritableComparable, textFourWritableComparable);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}