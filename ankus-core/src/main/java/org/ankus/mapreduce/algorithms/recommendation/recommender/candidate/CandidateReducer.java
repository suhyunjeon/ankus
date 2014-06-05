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
package org.ankus.mapreduce.algorithms.recommendation.recommender.candidate;

import org.ankus.io.TextTwoWritableComparable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * CandidateReducer
 * @desc
 *      User-based Collaborative Filtering recommendation algorithms
 *      Arrange Candidate items for n users
 * @version 0.0.1
 * @date : 2013.07.13
 * @author Suhyun Jeon
 */
public class CandidateReducer extends Reducer<Text, TextTwoWritableComparable, Text, Text> {

    private List<String> allItemList;
    private BufferedReader bufferedReader;

    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        String itemListPath = configuration.get(Constants.ITEM_LIST_HDFS_PATH);

        this.allItemList = new ArrayList<String>();

        try{
            FileSystem fileSystem = FileSystem.get(configuration);
            Path path = new Path(itemListPath + "/");
            FileStatus[] fileStatuses = fileSystem.listStatus(path);

            for (int i=0; i<fileStatuses.length; i++){
                if(fileStatuses[i].getPath().toString().matches("(.*)part-r(.*)")){
                    bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(fileStatuses[i].getPath()), Constants.UTF8));
                    String line;
                    line = bufferedReader.readLine();
                    while (line != null){
                        this.allItemList.add(line);
                        line = bufferedReader.readLine();
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            bufferedReader.close();
        }
    }

    @Override
    protected void reduce(Text key, Iterable<TextTwoWritableComparable> values, Context context) throws IOException, InterruptedException {

        List<String> itemList = new ArrayList<String>();
        Iterator<TextTwoWritableComparable> iterator = values.iterator();

        while (iterator.hasNext()){
            TextTwoWritableComparable textTwoWritableComparable = iterator.next();
            itemList.add(textTwoWritableComparable.getText1().toString());
        }

        // Get item list for me
        boolean flag = false;
        for (String hdfsItem : this.allItemList) {
            for (String item : itemList){
                if(!itemList.contains(hdfsItem)){
                    flag = false;
                }else{
                    flag = true;
                }
            }

            if(!flag){
                context.write(key, new Text(hdfsItem));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}