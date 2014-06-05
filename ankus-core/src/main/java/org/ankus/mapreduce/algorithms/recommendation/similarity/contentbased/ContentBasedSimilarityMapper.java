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

import org.ankus.io.TextTwoWritableComparable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ContentsBasedSimilarityMapper
 * @desc
 *      Here's an version of the similarity coefficient and distance calculation.
 *      1. Dice coefficient 2. Jaccard coefficient 3. Hamming distance
 *
 * Example dataset
 * ------------------------------------------------------------------------------------------------
 * #<id>::<title>::<genre>::<directors>::<actors,actresses>::<keywords>
 *   1::Toy Story (1995)::Adventure|Animation|Children|Comedy|Fantasy::Tasha::Suhyun|Moonie::animation
 *   2::Jumanji (1995)::Adventure|Children|Fantasy::SungJun::animation
 *   3::Grumpier Old Men (1995)::Comedy|Romance::Wonbin::Suhyun|Wonbin::animation
 *-------------------------------------------------------------------------------------------------
 * @return The is between the two input VECTOR boolean dataset..
 * 		   Returns 1 if one 0 or both of the booleans are not {@code 0 or 1}.
 *
 * @version 0.1
 * @date : 2013.09.25
 * @author Suhyun Jeon
 */
public class ContentBasedSimilarityMapper extends Mapper<LongWritable, Text, Text, TextTwoWritableComparable> {

    private String keyIndex;
    private String indexList;
    private String delimiter;
    private String subDelimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.keyIndex = configuration.get(Constants.KEY_INDEX);
        this.indexList = configuration.get(Constants.TARGET_INDEX);
        this.delimiter = configuration.get(Constants.DELIMITER);
        this.subDelimiter = configuration.get(Constants.SUB_DELIMITER);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString();
        String[] columns = row.split(delimiter);

        StringBuffer uniqueKeyStringBuffer = new StringBuffer();
        StringBuffer indexListStringBuffer = new StringBuffer();
        List<String> list = new ArrayList<String>();

        String[] indexListSplit = indexList.split(subDelimiter);

        for(int i=0; i<columns.length; i++){
            String column = columns[i];

            for (String data : indexListSplit) {

                if(i == Integer.parseInt(data)){
                    indexListStringBuffer.append(column + "\t");
                    list.add(column);
                }
            }

            if(i == Integer.parseInt(keyIndex)){
                uniqueKeyStringBuffer.append(column);
            }else{
                continue;
            }
        }

        TextTwoWritableComparable textTwoWritableComparable = null;
        for(int i=0; i<list.size(); i++){

            String[] subColumns1 = indexListStringBuffer.toString().split("\t");
            String[] subColumns2 = subColumns1[i].split(subDelimiter);

            for(int k=0; k<subColumns2.length; k++) {
                textTwoWritableComparable = new TextTwoWritableComparable(uniqueKeyStringBuffer.toString(), subColumns2[k]);
                context.write(new Text("feature-"+i), textTwoWritableComparable);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}