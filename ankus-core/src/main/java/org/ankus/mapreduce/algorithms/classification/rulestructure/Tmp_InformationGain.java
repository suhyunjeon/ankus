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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InformationGain
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.13
 * @author Moonie Song
 */
public class Tmp_InformationGain {

    private static double m_log2 = Math.log(2);

    // SLF4J Logging
    private static Logger logger = LoggerFactory.getLogger(Tmp_InformationGain.class);


    public static void main(String args[])
    {
        int sumArr[] = new int[3];
        double igArr[] = new double[3];

        int classDist_0[] = {2,3,0};
        sumArr[0] = 0;
        for(int c: classDist_0) sumArr[0] += c;
        igArr[0] = getInformationValue(classDist_0, sumArr[0]);
        logger.info("IG-1: " + igArr[0]);

        int classDist_1[] = {4,0,0,0,0,0,0,1};
        sumArr[1] = 0;
        for(int c: classDist_1) sumArr[1] += c;
        igArr[1] = getInformationValue(classDist_1, sumArr[1]);
        logger.info("IG-2: " + igArr[1]);

        int classDist_2[] = {3,3,0,3};
        sumArr[2] = 0;
        for(int c: classDist_2) sumArr[2] += c;
        igArr[2] = getInformationValue(classDist_2, sumArr[2]);
        logger.info("IG-3: " + igArr[2]);

        int totSum = 0;
        for(int c: sumArr) totSum += c;
        logger.info("E: " + getEntropy(sumArr, totSum, igArr));

    }

    public static double getInformationValue(int[] classDist, int sum)
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

    public static double getEntropy(int[] attrSumArr, int totalSum, double[] IGArr)
    {
        double val = 0.0;

        for(int i=0; i<attrSumArr.length; i++)
        {
            logger.info(attrSumArr[i] + " : " + IGArr[i]);
            val = val + ((double)attrSumArr[i] / (double)totalSum * IGArr[i]);
        }

        return val;
    }
}
