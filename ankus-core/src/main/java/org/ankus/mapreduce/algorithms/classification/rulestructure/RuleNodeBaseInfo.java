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

/**
 * RuleNodeBaseInfo
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.13
 * @author Moonie Song
 */
public class RuleNodeBaseInfo {

    public static String conditionDelimiter = "@@";
    public String attrCondition;
    private String attrValue;

    private int dataCnt;
    private double purity;
    private String classLabel;

    boolean isLeaf;

    public RuleNodeBaseInfo()
    {

    }

    public RuleNodeBaseInfo(String attrCondition, String attrValue, int dataCnt, double purity, String classLabel)
    {
        setNodeInfo(attrCondition, attrValue, dataCnt, purity, classLabel);
        setIsLeaf(false);
    }

    public void setNodeInfo(String attrCondition, String attrValue, int dataCnt, double purity, String classLabel)
    {
        this.attrCondition = attrCondition;
        this.attrValue = attrValue;
        this.dataCnt = dataCnt;
        this.purity = purity;
        this.classLabel = classLabel;
    }

    public void setIsLeaf(boolean isLeaf)
    {
        this.isLeaf = isLeaf;
    }

    public String toString(String delimiter)
    {
        String retVal = "";

        String condStr = "";
        int addIndex = this.attrValue.lastIndexOf(this.conditionDelimiter);
        if(addIndex < 0) condStr = this.attrCondition + this.conditionDelimiter + this.attrValue;
        else
        {
            condStr = this.attrValue.substring(0, addIndex)
                    + this.conditionDelimiter + this.attrCondition
                    + this.conditionDelimiter + this.attrValue.substring(addIndex + 2);
        }

        retVal = condStr
                + delimiter + this.dataCnt
                + delimiter + this.purity
                + delimiter + this.classLabel
                + delimiter + this.isLeaf;

        return retVal;
    }
}
