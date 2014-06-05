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
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;

import org.ankus.io.TextIntegerPairWritableComparable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * BooleanSetMapper
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
 * @version 0.0.1
 * @date : 2013.07.10
 * @author Suhyun Jeon
*/
public class BooleanSetMapper extends Mapper<LongWritable, Text, Text, TextIntegerPairWritableComparable> {

    private String keyIndex;
    private String delimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.keyIndex = configuration.get(Constants.KEY_INDEX);
        this.delimiter = configuration.get(Constants.DELIMITER);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       String row = value.toString();
       String[] columns = row.split(delimiter);
       StringBuffer uniqueKeyStringBuffer = new StringBuffer();

       for(int i=0; i<columns.length; i++){
           String column = columns[i];
           if(i == Integer.parseInt(keyIndex)){
               uniqueKeyStringBuffer.append(column);
           }else{
               continue;
           }
       }

       for(int i=1; i<columns.length; i++){
           // If the data value is not equal 0 or 1, the value is 1.
            if(columns[i].equals("0") || columns[i].equals("1")){
                value.set(columns[i]);
            }else{
                value.set("1");
            }
        	TextIntegerPairWritableComparable textIntegerPairWritableComparable = new TextIntegerPairWritableComparable(uniqueKeyStringBuffer.toString(), Integer.parseInt(value.toString()));
            context.write(new Text("item-" + i), textIntegerPairWritableComparable);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}