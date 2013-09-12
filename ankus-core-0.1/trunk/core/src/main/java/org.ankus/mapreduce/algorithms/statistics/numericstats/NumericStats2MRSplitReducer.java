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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.ankus.util.ConfigurationVariable;

/**
 * NumericStats2MRSplitReducer
 * @desc 1st reducer class for numeric statistics computation mr job (2-step)
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class NumericStats2MRSplitReducer extends Reducer<Text, Text, Text, Text>{

    private String delimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        // TODO
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
    }

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException 
	{
		Iterator<Text> iterator = values.iterator();
						
		int cnt = 0;
		double max = 0;
		double min = 0;
		double sum = 0;
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
				max = value;
				min = value;
			}
			else
			{
				if(max < value) max = value;
				if(min > value) min = value;
			}
			
			if(value<=0) allPositive = false;
			sum += value;
			if(allPositive)
			{
				harmonicSum += 1/value;
				geometricSum += Math.log10(value);		// for overflow
			}
			squareSum += Math.pow(value, 2) / 10000;	// for overflow
        }
        
        String writeValue = cnt + delimiter +
        				max + delimiter +
        				min + delimiter +
        				sum + delimiter +
        				harmonicSum + delimiter +
        				geometricSum + delimiter +
        				squareSum;
        if(allPositive) writeValue += delimiter + "T";
        else writeValue += delimiter + "F";
        		
        context.write(key, new Text(writeValue));
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
