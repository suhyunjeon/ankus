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

import org.ankus.mapreduce.algorithms.classification.rulestructure.RuleNodeBaseInfo;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * ID3AttributeSplitMapper
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.12
 * @author Moonie Song
 */
public class ID3AttributeSplitMapper extends Mapper<Object, Text, Text, Text>{

    String m_delimiter;
    String m_ruleCondition;
    int m_indexArr[];
    int m_numericIndexArr[];
    int m_exceptionIndexArr[];
    int m_classIndex;
    HashMap<Integer, String> m_conditionMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
        m_indexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        m_numericIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NUMERIC_INDEX, "-1"));
        m_exceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
        m_classIndex = Integer.parseInt(conf.get(ArgumentsConstants.CLASS_INDEX, "-1"));
        m_ruleCondition = conf.get(Constants.ID3_RULE_CONDITION, "root");

        if(m_ruleCondition.equals("root")) m_conditionMap = null;
        else
        {
            m_conditionMap = new HashMap<Integer, String>();

            String tokens[] = m_ruleCondition.split(RuleNodeBaseInfo.conditionDelimiter);
            for(int i=0; i<tokens.length; i++)
            {
                m_conditionMap.put(Integer.parseInt(tokens[i]), tokens[i+1]);
                i++;
            }
        }
    }

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
        String[] columns = value.toString().split(m_delimiter);

        if(m_conditionMap==null)
        {
            for(int i=0; i<columns.length; i++)
            {
                if(CommonMethods.isContainIndex(m_indexArr, i, true)
                        && !CommonMethods.isContainIndex(m_numericIndexArr, i, false)
                        && !CommonMethods.isContainIndex(m_exceptionIndexArr, i, false)
                        && (i!=m_classIndex))
                {
                    // attr-index, attr-value, class-value
                    context.write(new Text(i + ""), new Text(columns[i] + m_delimiter + columns[m_classIndex]));
                }
            }
        }
        else
        {
            Iterator keyIter = m_conditionMap.keySet().iterator();
            boolean match = true;
            while(keyIter.hasNext())
            {
                int keyAttrIndex = (Integer)keyIter.next();
                if(!m_conditionMap.get(keyAttrIndex).equals(columns[keyAttrIndex]))
                {
                    match = false;
                    break;
                }
            }

            if(match)
            {
                for(int i=0; i<columns.length; i++)
                {
                    if(CommonMethods.isContainIndex(m_indexArr, i, true)
                            && !CommonMethods.isContainIndex(m_numericIndexArr, i, false)
                            && !CommonMethods.isContainIndex(m_exceptionIndexArr, i, false)
                            && (i!=m_classIndex)
                            && !m_conditionMap.containsKey(i))
                    {
                        // attr-index, attr-value, class-value
                        context.write(new Text(i + ""), new Text(m_ruleCondition + RuleNodeBaseInfo.conditionDelimiter + columns[i]
                                                                + m_delimiter + columns[m_classIndex]));
                    }
                }

            }
        }
	}


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
