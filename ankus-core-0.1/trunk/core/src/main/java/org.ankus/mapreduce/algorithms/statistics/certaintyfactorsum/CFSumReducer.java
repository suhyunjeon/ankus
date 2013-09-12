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
package org.ankus.mapreduce.algorithms.statistics.certaintyfactorsum;

import java.io.IOException;
import java.util.Iterator;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * CFSumReducer
 * @desc (common) reducer class for certainty factor based summation mr job (1-step / 2-step)
 * @version 0.0.1
 * @date : 2013.08.20
 * @author Moonie
 */
public class CFSumReducer extends Reducer<Text, Text, NullWritable, Text>{

    // value for vf sum max
	private double sumMax;
	private String delimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        sumMax = Double.parseDouble(context.getConfiguration().get(ArgumentsConstants.CERTAINTY_FACTOR_MAX, "1"));
        // TODO '\t'을 변수명으로 수정해야 함
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
    }

    @Override
	protected void reduce(Text key, Iterable<Text> values, Context context)	throws IOException, InterruptedException
	{
		Iterator<Text> iterator = values.iterator();
		double m_sum = 0;

        while (iterator.hasNext())
        {
            double value = Double.parseDouble(iterator.next().toString());
            m_sum = m_sum + value - (m_sum * value / sumMax);
        }
        context.write(NullWritable.get(), new Text(key.toString() + delimiter + m_sum));
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
