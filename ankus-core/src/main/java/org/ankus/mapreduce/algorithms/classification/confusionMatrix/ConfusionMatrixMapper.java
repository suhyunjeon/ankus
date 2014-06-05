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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ID3AttributeSplitMapper
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.12
 * @author Moonie Song
 */
public class ConfusionMatrixMapper extends Mapper<Object, Text, Text, IntWritable>{

    String m_delimiter;
    IntWritable m_countOne;
    int m_classIndex;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
        m_countOne = new IntWritable(1);
        m_classIndex = Integer.parseInt(conf.get(ArgumentsConstants.CLASS_INDEX, "-1"));
    }

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
        String[] columns = value.toString().split(m_delimiter);

        int orgLabelIndex = m_classIndex;
        int newLabelIndex = columns.length -1;

        context.write(new Text(columns[orgLabelIndex] + m_delimiter + columns[newLabelIndex]), m_countOne);

	}


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
