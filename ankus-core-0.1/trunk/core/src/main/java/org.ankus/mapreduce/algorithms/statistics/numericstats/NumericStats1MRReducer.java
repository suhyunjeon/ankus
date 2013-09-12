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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * NumericStats1MRReducer
 * @desc reducer class for numeric statistics computation mr job (1-step)
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class NumericStats1MRReducer extends Reducer<IntWritable, Text, NullWritable, Text>{

	private String delimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
    }

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		Iterator<Text> iterator = values.iterator();
						
		int cnt = 0;
		double sum = 0;
		double avg = 0;
		double avgGeometric = 0;
		double avgHarmonic = 0;
		double variance = 0;
		double stdDeviation = 0;		
		double maxData = 0;
		double minData = 0;
		double middleData_Value = 0;		
		double harmonicSum = 0;
		double geometricSum = 0;
		double squareSum = 0;
		boolean allPositive = true;	
		
        while (iterator.hasNext()) 
        {
        	double value = Double.parseDouble(iterator.next().toString());
        	cnt++;
        	
        	if(cnt==1)
			{
				maxData = value;
				minData = value;
			}
			else
			{
				if(maxData < value) maxData = value;
				if(minData > value) minData = value;
			}
			
			if(value <= 0) allPositive = false;
			sum += value;
			if(allPositive)
			{
				harmonicSum += 1/value;
				geometricSum += Math.log10(value);
			}
			squareSum += Math.pow(value, 2) / 10000;
        }
        
        avg = sum / (double)cnt;		
		if(allPositive)
		{
			avgHarmonic = (double)cnt / harmonicSum;
			avgGeometric = Math.pow(10, geometricSum / (double)cnt);
		}
		else
		{
			avgHarmonic = 0;
			avgGeometric = 0;
		}
		
		variance = (squareSum * 10000 /(double)cnt) - Math.pow(avg,2);
		stdDeviation = Math.sqrt(variance);		
		middleData_Value = (maxData + minData) / 2;

        String writeVal = sum + delimiter +
				avg + delimiter +
				avgHarmonic + delimiter +
				avgGeometric + delimiter +
				variance + delimiter +
				stdDeviation + delimiter +
				maxData + delimiter +
				minData + delimiter +
				middleData_Value;
        context.write(NullWritable.get(), new Text(key.toString() + delimiter + writeVal));
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
