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
package org.ankus.mapreduce.algorithms.clustering.em;

import java.io.IOException;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.ankus.util.ConfigurationVariable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EMClusterAssignMapper extends Mapper<Object, Text, IntWritable, Text>{

	String m_delimiter;

	int m_indexArr[];
	int m_nominalIndexArr[];
	int m_exceptionIndexArr[];

	int m_clusterCnt;
	EMClusterInfoMgr clusters[];

    boolean isInitial;


	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{

	}

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
        int clusterIndex = -1;

        if(isInitial)
        {
            clusterIndex = (int) (Math.random() * m_clusterCnt);
        }
        else
        {
            String[] columns = value.toString().split(m_delimiter);

            /**
             * cluster index get
             */
            double probMax = 0;
            for(int k=0; k<m_clusterCnt; k++)
            {
                double attrProbSum = 1;
                double attrCnt = 0;

                /**
                 * TODO: total distance - probability product sum
                 */
                for(int i=0; i<columns.length; i++)
                {
                    double probAttr = 0;

                    if(CommonMethods.isContainIndex(m_indexArr, i, true)
                            && !CommonMethods.isContainIndex(m_exceptionIndexArr, i, false))
                    {
                        attrCnt = attrCnt + 1;
                        if(CommonMethods.isContainIndex(m_nominalIndexArr, i, false))
                        {
                            probAttr = clusters[k].getAttributeProbability(i, columns[i], ConfigurationVariable.NOMINAL_ATTRIBUTE);
                        }
                        else probAttr = clusters[k].getAttributeProbability(i, columns[i], ConfigurationVariable.NUMERIC_ATTRIBUTE);

    //					attrProbSum += probAttr;
                        attrProbSum *= probAttr;
                    }

                }

    //			double prob = attrProbSum / attrCnt;
                double prob = attrProbSum;
                if(prob >= probMax)
                {
                    probMax = prob;
                    clusterIndex = k;
                }
            }
            // probability normalizing
        }


		context.write(new IntWritable(clusterIndex), value);
	}


	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();

		m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");

		m_indexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX,  "-1"));
		m_nominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1"));
		m_exceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));

		m_clusterCnt = Integer.parseInt(conf.get(ArgumentsConstants.CLUSTER_COUNT, "1"));


        if(conf.get("IS_INITIAL", "FALSE").equals("TRUE")) isInitial = true;
        else
        {
            isInitial = false;
            // cluster load and setting
            Path clusterPath = new Path(conf.get(ArgumentsConstants.CLUSTER_PATH, null));
            clusters = EMClusterInfoMgr.loadClusterInfoFile(conf, clusterPath, m_clusterCnt, m_delimiter);
        }
	}

	

}
