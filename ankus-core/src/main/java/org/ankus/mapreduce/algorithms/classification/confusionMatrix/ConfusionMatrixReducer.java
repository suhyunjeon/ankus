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
package org.ankus.mapreduce.algorithms.classification.confusionMatrix;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * ID3ComputeEntropyReducer
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.12
 * @author Moonie Song
 */
public class ConfusionMatrixReducer extends Reducer<Text, IntWritable, NullWritable, Text>{

    String m_delimiter;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");

    }

//	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
        Iterator<IntWritable> iterator = values.iterator();

        int totalCnt = 0;
        while (iterator.hasNext())
        {
            iterator.next();
            totalCnt++;
        }

        context.write(NullWritable.get(), new Text(key.toString() + m_delimiter + totalCnt));
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
