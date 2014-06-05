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
 * RuleTreeMgr
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.13
 * @author Moonie Song
 */
public class Tmp_RuleTreeMgr {

    private String levelString = "|";
    private String infoDelimiter = "\t";
    private String valueDelimiter = "=";


    private String[] classLabelArr = null;

    public Tmp_RuleTree loadTree(String[] readLines)
    {
        /**
         *  load tree from readlines
         *   - first line is class info
          */

        return null;
    }

    public void setClassLabelArr(String classListStr)
    {
        classLabelArr = classListStr.split(",");
    }

    public void add(Tmp_RuleTree parent, Tmp_RuleTree child)
    {
        if(parent.getLeftChild() == null) parent.setLeftChild(child);
        else
        {
            Tmp_RuleTree temp = parent.getLeftChild();
            while(temp.getRightSibling() != null) temp = temp.getRightSibling();
            temp.setRightSibling(child);
        }
    }

    public Tmp_RuleTree[] getLeafNodes(Tmp_RuleTree parentNode)
    {

        return null;
    }


    public String getCurrentNodeInfoStr(Tmp_RuleTree node)
    {

        String printStr = node.getName() + valueDelimiter + node.getValue();

        if(node.getLeftChild() != null)
        {
            printStr += infoDelimiter + classLabelArr[node.getClassIndex()];

            int[] dataCntArr = node.getDataCntArr();
            /**
             * dataCntArr -> toString
             */



            return printStr;
        }
        else return printStr;
    }





    /**
     * Reference Codes..
     */
    public void printLevelNodes(Tmp_RuleTree node, int level)
    {
        int depth = 0;
        Tmp_RuleTree tempChild = node;
        Tmp_RuleTree tempParent = node;

        while(depth <= level)
        {
            if(depth == level)
            {
                while(tempChild != null)
                {
                    System.out.print(getCurrentNodeInfoStr(tempChild));
                    tempChild = tempChild.getRightSibling();
                }

                if(tempParent.getRightSibling() != null)
                {
                    tempParent = tempParent.getRightSibling();
                    tempChild = tempParent.getLeftChild();
                } else break;
            }
            else
            {
                tempParent = tempChild;
                tempChild = tempChild.getLeftChild();
                depth++;
            }
        }
    }

    public void printChildrenNodes(Tmp_RuleTree node, int depth)
    {
        for(int i = 0; i < depth; i++)
            if(node.getLeftChild() != null)
                printChildrenNodes(node.getLeftChild(), depth + 1);

            if(node.getRightSibling() != null)
                printChildrenNodes(node.getRightSibling(), depth);
    }


}
