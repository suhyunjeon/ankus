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

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * CFSum2MRMergeMapper
 * @desc 1st mapper class for certainty factor based summation mr job (2-step)
 *
 * @version 0.0.1
 * @date : 2013.08.20
 * @author Moonie
 */
public class CFSum2MRMergeMapper extends Mapper<Object, Text, Text, Text>{

	private String delimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        // TODO '\t'을 변수명으로 수정해야 함
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
    }

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		String valueStr = value.toString();
		int splitIndex = valueStr.indexOf(delimiter);
		
		String keyStr = valueStr.substring(0, splitIndex);
		keyStr = keyStr.substring(0, keyStr.indexOf("_"));
		
		valueStr = valueStr.substring(splitIndex + 1);
		context.write(new Text(keyStr), new Text(valueStr));
	}

    @Override
    protected void cleanup(Context context)	throws IOException, InterruptedException
    {
    }

}
