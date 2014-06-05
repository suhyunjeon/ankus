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
package org.ankus.mapreduce.algorithms.classification.rulestructure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * EntropyInfo
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.13
 * @author Moonie Song
 */
public class EntropyInfo {

    public double entropy = 1.0;

    public ArrayList<String> attrValueList = new ArrayList<String>();
    public ArrayList<Integer> attrValueTotalCntList = new ArrayList<Integer>();
    public ArrayList<Double> attrValuePurityList = new ArrayList<Double>();
    public ArrayList<String> attrMaxClassList = new ArrayList<String>();

    public void setValueList(ArrayList<String> valueList)
    {
        attrValueList.addAll(valueList);
    }

    public void addAttributeDist(int sumArr[], int maxArr[], String classArr[])
    {
        int len = sumArr.length;

        for(int i=0; i<len; i++)
        {
            attrValueTotalCntList.add(sumArr[i]);
            attrValuePurityList.add((double)maxArr[i]/(double)sumArr[i]);
            attrMaxClassList.add(classArr[i]);
        }
    }

    public String toString(String delimiter)
    {

        String str = "" + entropy;

        int len = attrValueList.size();
        for(int i=0; i<len; i++)
        {
            str += delimiter + attrValueList.get(i)
                    + delimiter + attrValueTotalCntList.get(i)
                    + delimiter + attrValuePurityList.get(i)
                    + delimiter + attrMaxClassList.get(i);
        }

        return str;
    }
}
