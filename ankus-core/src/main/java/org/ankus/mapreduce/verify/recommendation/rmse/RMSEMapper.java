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
package org.ankus.mapreduce.verify.recommendation.rmse;

import java.io.IOException;

import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * RMSEMapper
 * @desc
 *      Calculating RMSE(base) by ankus recommendation system.
 *      This is rearrange recommendation system. There is no candidate step.
 *      Only support User-based Collaborative Filtering recommendation algorithms
 *      Runs a recommendation job as a series of map/reduce
 * @version 0.1
 * @date : 2013.11.18
 * @author Suhyun Jeon
 */
public class RMSEMapper extends Mapper<LongWritable, Text, NullWritable, DoubleWritable> {

    private String delimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        delimiter = configuration.get(Constants.DELIMITER);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString();
        String[] columns = row.split(delimiter);

        double originalRatingOfPrediction = Double.parseDouble(columns[2]);
        double ratingOfPrediction = 0.0d;

        try{
            if(columns[3].isEmpty()) {
                ratingOfPrediction = originalRatingOfPrediction;
                ratingOfPrediction = 0;
            } else {
                ratingOfPrediction = Double.parseDouble(columns[3]);
            }
        }catch (ArrayIndexOutOfBoundsException e){
            e.printStackTrace();
        }

        double diff = originalRatingOfPrediction - ratingOfPrediction;

        context.write(NullWritable.get(), new DoubleWritable(diff));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}