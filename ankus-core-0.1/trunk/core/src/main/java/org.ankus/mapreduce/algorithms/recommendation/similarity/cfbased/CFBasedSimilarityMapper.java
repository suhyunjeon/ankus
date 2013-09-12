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

import org.ankus.io.TextDoublePairWritableComparable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * CFBasedSimilarityMapper
 * @desc
 *      User-based Collaborative Filtering recommendation algorithms
 *      1. Cosine coefficient 2. Pearson coefficient
 *      Required data set : [userID, itemID, rating]
 * @version 0.0.1
 * @date : 2013.07.20
 * @author Suhyun Jeon
 */
public class CFBasedSimilarityMapper extends Mapper<LongWritable, Text, Text, TextDoublePairWritableComparable> {

    private String delimiter;
	private String basedType;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.basedType = configuration.get(Constants.BASED_TYPE);
        this.delimiter = configuration.get(Constants.DELIMITER);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    	String row = value.toString();
        String[] columns = row.split(delimiter);

        StringBuffer keyStringBuffer = new StringBuffer();
        StringBuffer nonKeyStringBuffer = new StringBuffer();
        Double rating = Double.parseDouble(columns[2]);

        if(basedType.equals(Constants.USER_BASED)){
            keyStringBuffer.append(columns[0]);
            nonKeyStringBuffer.append(columns[1]);
        }else if(basedType.equals(Constants.ITEM_BASED)){
            keyStringBuffer.append(columns[1]);
            nonKeyStringBuffer.append(columns[0]);
        }

        TextDoublePairWritableComparable textDoublePairWritableComparable = new TextDoublePairWritableComparable(keyStringBuffer.toString(), rating);
	  	context.write(new Text(nonKeyStringBuffer.toString()), textDoublePairWritableComparable);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
