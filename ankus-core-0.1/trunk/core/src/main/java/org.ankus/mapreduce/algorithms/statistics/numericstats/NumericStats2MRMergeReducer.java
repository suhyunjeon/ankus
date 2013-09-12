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
package org.ankus.mapreduce.algorithms.statistics.numericstats;

import java.io.IOException;
import java.util.Iterator;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * NumericStats2MRMergeReducer
 * @desc 2nd reducer class for numeric statistics computation mr job (2-step)
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class NumericStats2MRMergeReducer extends Reducer<Text, Text, NullWritable, Text>{

	private String delimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        // TODO
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
    }


//	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException 
	{
		int cnt = 0;
		double max = 0;
		double min = 0;
		double sum = 0;
		double harmonic_sum = 0;
		double geometric_sum = 0;
		double square_sum = 0;			
		boolean allPositive = true;
		
		Iterator<Text> iterator = values.iterator();		
		while (iterator.hasNext()) 
        {
			String tokens[] = iterator.next().toString().split(delimiter);
			
			int curCnt = Integer.parseInt(tokens[0]);
			double curMax = Double.parseDouble(tokens[1]);
			double curMin = Double.parseDouble(tokens[2]);
			cnt += curCnt;
        	if(cnt==curCnt)
			{
				max = curMax;
				min = curMin;
			}
			else
			{
				if(max < curMax) max = curMax;
				if(min > curMin) min = curMin;
			}
			
			if(tokens[7].equals("F")) allPositive = false;
			sum += Double.parseDouble(tokens[3]);
			if(allPositive)
			{
				harmonic_sum += Double.parseDouble(tokens[4]);
				geometric_sum += Double.parseDouble(tokens[5]);
			}
			square_sum += Double.parseDouble(tokens[6]);
        }
		
		double avg = sum / (double)cnt;
		double avg_harmonic = 0;
		double avg_geometric = 0;
		if(allPositive)
		{
			avg_harmonic = (double)cnt / harmonic_sum;
			avg_geometric = Math.pow(10, geometric_sum /(double)cnt);
		}
		
		double variance = (square_sum * 10000 /(double)cnt) - Math.pow(avg,2);
		double stdDeviation = Math.sqrt(variance);		
		double middleData_Value = (max + min) / 2;
		
		String writeVal = sum + delimiter +
							avg + delimiter +
							avg_harmonic + delimiter +
							avg_geometric + delimiter +
							variance + delimiter +
							stdDeviation + delimiter +
							max + delimiter +
							min + delimiter +
							middleData_Value;
		context.write(NullWritable.get(), new Text(key.toString() + delimiter + writeVal));
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
