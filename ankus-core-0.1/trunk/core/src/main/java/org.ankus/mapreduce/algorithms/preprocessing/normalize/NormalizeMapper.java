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
package org.ankus.mapreduce.algorithms.preprocessing.normalize;

import java.io.IOException;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.ankus.util.ConfigurationVariable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * NormalizeMapper
 * @desc mapper class for normalization mr job
 *
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class NormalizeMapper extends Mapper<Object, Text, NullWritable, Text>{

    // delimiter for attribute separation
	private String mDelimiter;
    // attribute index array for normalization
	private int mIndexArr[];
    // value for if no-normalization attributes is remain
	private boolean mRemainFields;
    // attribute index array for do not normalize
	private int mExceptionIndexArr[];

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        mDelimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
        mIndexArr = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.TARGET_INDEX,  "-1"));
        mExceptionIndexArr = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));

        if(context.getConfiguration().get(ArgumentsConstants.REMAIN_FIELDS, "true").equals("true")){
            mRemainFields = true;
        }
        else
        {
            mRemainFields = false;
        }
    }

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] columns = value.toString().split(mDelimiter);
		
		String writeVal = "";
		for(int i=0; i<columns.length; i++)
		{
			if(CommonMethods.isContainIndex(mIndexArr, i, true))
			{
				if(!CommonMethods.isContainIndex(mExceptionIndexArr, i, false))
				{
					if(i>0) writeVal += mDelimiter;
					
					String minMax[] = context.getConfiguration().get(ConfigurationVariable.MINMAX_VALUE + "_" + i, "0,0").split(",");
					
					double val1 = Double.parseDouble(columns[i])-Double.parseDouble(minMax[0]);							
					double val2 = Double.parseDouble(minMax[1])-Double.parseDouble(minMax[0]);
					
					if((val2==0)||(val2==0)) writeVal += "0";
					else writeVal += "" + (val1 / val2);
				}
				else if(mRemainFields)
				{
					if(i > 0) writeVal += mDelimiter;
					writeVal += columns[i];
				}
			}
			else if(mRemainFields)
			{
				if(i>0) writeVal += mDelimiter;
				writeVal += columns[i];
			}
		}
		context.write(NullWritable.get(), new Text(writeVal.trim()));
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
