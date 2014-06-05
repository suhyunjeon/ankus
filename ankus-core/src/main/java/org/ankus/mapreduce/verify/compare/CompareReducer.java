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
package org.ankus.mapreduce.verify.compare;

import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * CompareReducer
 *
 * @author Suhyun Jeon
 * @version 0.1
 * @desc
 *      Compare data set between original data set(ex. movielens) and recommendation result data set.
 *      1	1	3.913	5
 *      1	101	3.474	2
 *      1	105	2.739	2
 * @date : 2013.11.18
 * @author Suhyun Jeon
 */
public class CompareReducer extends Reducer<Text, Text, Text, Text> {

    private String delimiter;

	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.delimiter = configuration.get(Constants.DELIMITER);
    }  

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	Iterator<Text> iterator = values.iterator();

        List<String> recommendResultList = new ArrayList<String>();
        List<String> testDataSetList = new ArrayList<String>();

        List<String> recommendKeyList = new ArrayList<String>();
        List<String> testDataSetKeyList = new ArrayList<String>();

        while (iterator.hasNext()){
            Text record = iterator.next();
            String[] columns = record.toString().split(delimiter);

            if(columns[0].equals(Constants.RECOMMENDED)){
                recommendResultList.add(columns[1]);
                recommendKeyList.add(key.toString());
            }else{
                testDataSetList.add(columns[1]);
                testDataSetKeyList.add(key.toString());
            }
        }

        for(int i=0; i<testDataSetList.size(); i++) {
            context.write(key, new Text(testDataSetList.get(i) + "\t" + recommendResultList.get(i)));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}