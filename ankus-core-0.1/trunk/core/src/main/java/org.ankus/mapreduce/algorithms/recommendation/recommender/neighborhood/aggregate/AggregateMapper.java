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
package org.ankus.mapreduce.algorithms.recommendation.recommender.neighborhood.aggregate;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * AggregateMapper
 * @desc
 *      Aggregate for two input files.
 * @version 0.0.1
 * @date : 2013.09.02
 * @author Suhyun Jeon
 */
public class AggregateMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), values);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}