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
package org.ankus.mapreduce.algorithms.classification.id3;

import org.ankus.mapreduce.algorithms.classification.rulestructure.EntropyInfo;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * ID3ComputeEntropyReducer
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.12
 * @author Moonie Song
 */
public class ID3ComputeEntropyReducer extends Reducer<Text, Text, NullWritable, Text>{

    int m_indexArr[];
    int m_numericIndexArr[];
    int m_exceptionIndexArr[];
    int m_classIndex;
    double m_log2 = Math.log(2);
    String m_delimiter;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
        m_indexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        m_numericIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NUMERIC_INDEX, "-1"));
        m_exceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
        m_classIndex = Integer.parseInt(conf.get(ArgumentsConstants.CLASS_INDEX, "-1"));
    }

//	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
        Iterator<Text> iterator = values.iterator();

        HashMap<String, HashMap<String, Integer>> attrClassList = new HashMap<String, HashMap<String, Integer>>();
        ArrayList<String> valueList = new ArrayList<String>();
        while (iterator.hasNext())
        {
            String tokens[] = iterator.next().toString().split(m_delimiter);

            if(attrClassList.containsKey(tokens[0]))
            {
                HashMap classMap = attrClassList.get(tokens[0]);
                if(classMap.containsKey(tokens[1]))
                    classMap.put(tokens[1], (Integer)classMap.get(tokens[1])+1);
                else classMap.put(tokens[1], 1);

                attrClassList.put(tokens[0], classMap);
            }
            else
            {
                HashMap<String, Integer> classMap = new HashMap<String, Integer>();
                classMap.put(tokens[1], 1);
                attrClassList.put(tokens[0], classMap);

                valueList.add(tokens[0]);
            }
        }

        EntropyInfo e = new EntropyInfo();
        e.setValueList(valueList);
        computeIGVal(attrClassList, e);

        // attr-index, entropy, [attr-value, data-cnt, purity, class], ...
        context.write(NullWritable.get(), new Text(key.toString() + m_delimiter + e.toString(m_delimiter)));
	}

    private void computeIGVal(HashMap<String, HashMap<String, Integer>> attrClassList, EntropyInfo e)
    {
        int attrSize = attrClassList.size();

        int sumArr[] = new int[attrSize];
        double igArr[] = new double[attrSize];
        int maxArr[] = new int[attrSize];
        String classArr[] = new String[attrSize];
        int totSum = 0;

        for(int i=0; i<attrSize; i++)
        {
            String valueStr = e.attrValueList.get(i);
            HashMap<String, Integer> classDistList = attrClassList.get(valueStr);

            int classCnt = classDistList.size();
            Iterator<String> classIter = classDistList.keySet().iterator();
            sumArr[i] = 0;
            int classArrInt[] = new int[classCnt];
            int iterIndex = 0;
            while(classIter.hasNext())
            {
                String classVal = classIter.next().toString();
                classArrInt[iterIndex] = classDistList.get(classVal);
                sumArr[i] += classArrInt[iterIndex];
                if(maxArr[i] < classArrInt[iterIndex])
                {
                    maxArr[i] = classArrInt[iterIndex];
                    classArr[i] = classVal;
                }
                iterIndex++;
            }

            igArr[i] = getInformationValue(classArrInt, sumArr[i]);
            totSum += sumArr[i];
        }

        e.addAttributeDist(sumArr, maxArr, classArr);
        e.entropy = getEntropy(sumArr, totSum, igArr);
    }

    private double getInformationValue(int[] classDist, int sum)
    {
        double val = 0.0;
        for(int c: classDist)
        {
            double p = (double)c/(double)sum;
            if(c > 0) val = val + (p * Math.log(p)/m_log2);
        }

        if(val==0) return 0;
        else return val * -1;
    }

    private double getEntropy(int[] attrSumArr, int totalSum, double[] IGArr)
    {
        double val = 0.0;
        for(int i=0; i<attrSumArr.length; i++)
        {
            val = val + ((double)attrSumArr[i] / (double)totalSum * IGArr[i]);
        }

        return val;
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
