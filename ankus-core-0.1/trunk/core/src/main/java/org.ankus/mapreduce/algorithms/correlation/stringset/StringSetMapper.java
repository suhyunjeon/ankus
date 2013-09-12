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

import org.ankus.io.TextTwoWritableComparable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * StringSetMapper
 * @desc
 *      Here's an version of the similarity coefficient and distance calculation.
 *      1. Hamming distance 2. Edit distance
 * @version 0.0.1
 * @date : 2013.07.11
 * @author Suhyun Jeon
*/
public class StringSetMapper extends Mapper<LongWritable, Text, Text, TextTwoWritableComparable> {

	private String computeIndex;
    private String keyIndex;
    private String delimiter;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.computeIndex = configuration.get(Constants.COMPUTE_INDEX);
        this.keyIndex = configuration.get(Constants.KEY_INDEX);
        this.delimiter = configuration.get(Constants.DELIMITER);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       String row = value.toString();
       String[] columns = row.split(delimiter);
       
       StringBuffer keyIndexStringBuffer = new StringBuffer();
       StringBuffer computeIndexStringBuffer = new StringBuffer();
       
       for(int i=0; i<columns.length; i++){
    	   String column = columns[i];
    	   if(i == Integer.parseInt(keyIndex)){    
    		   keyIndexStringBuffer.append(column);
    	   }else if(i == Integer.parseInt(computeIndex)){
    		   computeIndexStringBuffer.append(column);
    	   }else{
    		   continue;
    	   }
	   }
       
       for(int k=1; k<columns.length; k++){
        	value.set(columns[k]);
           TextTwoWritableComparable textTwoWritableComparable = new TextTwoWritableComparable(keyIndexStringBuffer.toString(), computeIndexStringBuffer.toString());
        	context.write(new Text("item-" + k), textTwoWritableComparable);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}