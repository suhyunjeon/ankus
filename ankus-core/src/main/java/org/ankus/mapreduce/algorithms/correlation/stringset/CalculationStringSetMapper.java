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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * CalculationStringSetMapper
 * @desc
 *      Here's an version of the similarity coefficient and distance calculation.
 *      1. Hamming distance 2. Edit distance
 * @version 0.0.1
 * @date : 2013.07.11
 * @author Suhyun Jeon
*/
public class CalculationStringSetMapper extends Mapper<LongWritable, Text, TextTwoWritableComparable, TextFourWritableComparable> {

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
        String valueID1 = columns[3];
        String valueID2 = columns[4];

        TextTwoWritableComparable textTwoWritableComparable = new TextTwoWritableComparable(id1, id2);
        TextFourWritableComparable textFourWritableComparable = new TextFourWritableComparable(item, valueID1, item, valueID2);

        context.write(textTwoWritableComparable, textFourWritableComparable);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}