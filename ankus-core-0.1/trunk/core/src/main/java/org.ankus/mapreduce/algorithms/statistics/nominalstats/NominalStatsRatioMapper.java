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
package org.ankus.mapreduce.algorithms.statistics.nominalstats;

import java.io.IOException;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.ankus.util.ConfigurationVariable;

/**
 * NominalStatsRatioMapper
 * @desc 1st mapper class for nominal statistics computation mr job (2-step)
 * @version 0.0.1
 * @date : 2013.08.20
 * @author Moonie
 */
public class NominalStatsRatioMapper extends Mapper<Object, Text, NullWritable, Text>{
	
	private String delimiter;
    // total count for frequency ratio
	private long totalMapRecords;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        // TODO
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
        totalMapRecords = context.getConfiguration().getLong(ConfigurationVariable.MAP_OUTPUT_RECORDS_CNT, 0);
    }

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{		
		String valStr = value.toString();
		long val = Long.parseLong(valStr.substring(valStr.indexOf(",") + 1));
		double rate = (double)val / (double) totalMapRecords;

        context.write(NullWritable.get(), new Text(valStr + delimiter + rate));
	}	

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
