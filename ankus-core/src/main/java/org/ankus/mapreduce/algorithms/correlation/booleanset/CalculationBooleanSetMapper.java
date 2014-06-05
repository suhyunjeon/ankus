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

import org.ankus.io.TextIntegerTwoPairsWritableComparable;
import org.ankus.io.TextTwoWritableComparable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
 * @version 0.0.1
 * @date : 2013.07.10
 * @author Suhyun Jeon
*/
public class CalculationBooleanSetMapper extends Mapper<LongWritable, Text, TextTwoWritableComparable, TextIntegerTwoPairsWritableComparable> {

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

        String id1 = columns[0];
        String id2 = columns[1];
        String item = columns[2];
        Integer valueID1 = Integer.parseInt(columns[3]);
        Integer valueID2 = Integer.parseInt(columns[4]);

        TextTwoWritableComparable textTwoWritableComparable = new TextTwoWritableComparable(id1, id2);
        TextIntegerTwoPairsWritableComparable textIntegerTwoPairsWritableComparable = new TextIntegerTwoPairsWritableComparable(item, valueID1.intValue(), item, valueID2.intValue());

        context.write(textTwoWritableComparable, textIntegerTwoPairsWritableComparable);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}