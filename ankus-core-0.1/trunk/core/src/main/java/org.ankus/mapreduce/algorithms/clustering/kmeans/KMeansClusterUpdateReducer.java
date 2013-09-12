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
package org.ankus.mapreduce.algorithms.clustering.kmeans;

import java.io.IOException;
import java.util.Iterator;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.ankus.util.ConfigurationVariable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * KMeansClusterUpdateReducer
 * @desc reducer class for k-means mr job
 *
 * @version 0.0.1
 * @date : 2013.08.22
 * @author Moonie
 */
public class KMeansClusterUpdateReducer extends Reducer<IntWritable, Text, NullWritable, Text>{

	String mDelimiter;              // delimiter for attribute separation
	
	int mIndexArr[];                // index array used as clustering feature
	int mNominalIndexArr[];         // index array of nominal attributes used as clustering features
	int mExceptionIndexArr[];       // index array do not used as clustering features
	
	int mClusterCnt;                // cluster count




    @Override
	protected void cleanup(Context context) throws IOException, InterruptedException 
	{

	}

//	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException 
	{
		Iterator<Text> iterator = values.iterator();
				
		KMeansClusterInfoMgr cluster = new KMeansClusterInfoMgr();
		cluster.setClusterID(key.get());
		int dataCnt = 0;
		while (iterator.hasNext())
		{
			dataCnt++;
			String tokens[] = iterator.next().toString().split(mDelimiter);
			
			for(int i=0; i<tokens.length; i++)
			{
				if(CommonMethods.isContainIndex(mIndexArr, i, true)
						&& !CommonMethods.isContainIndex(mExceptionIndexArr, i, false))
				{
					if(CommonMethods.isContainIndex(mNominalIndexArr, i, false))
					{
						cluster.addAttributeValue(i, tokens[i], ConfigurationVariable.NOMINAL_ATTRIBUTE);
					}
					else cluster.addAttributeValue(i, tokens[i], ConfigurationVariable.NUMERIC_ATTRIBUTE);
				}
			}
		}
		cluster.finalCompute(dataCnt);
		
		String writeStr = cluster.getClusterInfoString(mDelimiter, context.getConfiguration().get("subDelimiter", "@@"));
        context.write(NullWritable.get(), new Text(writeStr));
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException 
	{
		Configuration conf = context.getConfiguration();
		
		mDelimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
		
		mIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX,  "-1"));
		mNominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1"));
		mExceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));
		
		mClusterCnt = Integer.parseInt(conf.get(ArgumentsConstants.CLUSTER_COUNT, "1"));
	}

	
	
	
	

}
