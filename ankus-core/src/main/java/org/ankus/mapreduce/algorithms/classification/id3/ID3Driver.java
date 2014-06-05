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

import org.ankus.mapreduce.algorithms.classification.confusionMatrix.ConfusionMatrixMapper;
import org.ankus.mapreduce.algorithms.classification.confusionMatrix.ConfusionMatrixReducer;
import org.ankus.mapreduce.algorithms.classification.rulestructure.RuleNodeBaseInfo;
import org.ankus.mapreduce.algorithms.classification.confusionMatrix.ValidationMain;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.ConfigurationVariable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

/**
 * ID3Driver
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.12
 * @author Moonie Song
 */
public class ID3Driver extends Configured implements Tool {

    private Logger logger = LoggerFactory.getLogger(ID3Driver.class);
    private String m_minData = "1";
    private String m_purity = "0.75";

    public int run(String[] args) throws Exception
    {
        Configuration conf = this.getConf();
        if(!ConfigurationVariable.setFromArguments(args, conf))
        {
            logger.info("> MR Job Setting Failed..");
            return 1;
        }

        if(!conf.get(ArgumentsConstants.NUMERIC_INDEX, "-1").equals("-1"))
        {
            logger.info("> ID3(Ankus) is not support Numeric Attribute.");
            logger.info("> Only Support Nominal Attribute.");
            return 1;
        }

        if(conf.get(ArgumentsConstants.CLASS_INDEX, "-1").equals("-1"))
        {
            logger.info("> Class Index is must defined in ID3(Ankus).");
            return 1;
        }
        /**
         * process
         *
         * parameter
         *      indexList, nominalIndexList, exceptionIndexList
         *      classIndex
         *
         *      minNodeCount, node-purity
         *
         * define
         *      tree(rule) structure
         *
         *          node = none
         *          leaf = major class label, data count for every class
         *
         * step-0
         *      class value-index setting
         * step-1
         *      get target-leaf node (check finish condition)
         * step-2
         *      for every target-leaf node (get parent condition) -> MR job
         *          for every feature, computes information gain -> or MR job
         *          select max ig-feature
         *          add leaf nodes based on selected feature
         *      go to step-1
         *
         *
         * MR
         *  - Map: K=attr-index, V=attr-value & class
         *  - Reduce: Compute IG
         *  Select Attr-index that have highest IG = S-A
         *
         *  Classification -> dara distribution -> check continue
         *  - Map: K=attr-index:value, ...attr-index:value
         *
         *
         *
         *
         *  - Map: K=attr-index:value, ...attr-index, V=attr-value & class
         *  - Reduce: Compute IG
         *  Select Attr-index that have highest IG in S-A = S-A..
         */

       /**
        outlook = sunny
        |  humidity = high: no
        |  humidity = normal: yes
        outlook = overcast: yes
        outlook = rainy
        |  windy = TRUE: no
        |  windy = FALSE: yes
         */
        String outputBase = conf.get(ArgumentsConstants.OUTPUT_PATH, null);
        String ruleFilePath = outputBase + "/id3_rule";
        conf.set(ArgumentsConstants.MIN_LEAF_DATA, conf.get(ArgumentsConstants.MIN_LEAF_DATA, m_minData));
        conf.set(ArgumentsConstants.PURITY, conf.get(ArgumentsConstants.PURITY, m_purity));
        String delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
        conf.set(ArgumentsConstants.DELIMITER, delimiter);

        logger.info("> ID3 Classification Iterations are Started..");
        logger.info("> : Information Gain Computation and Rule Update for Every Tree Node");
        // routine - start
        int iterCnt = 0;
        String nodeStr;
        while((nodeStr = loadNonLeafNode(conf))!=null)
        {
            /**
                << depth first process >>

                load rule file newly
                search non-leaf node
                if(no non-leaf node) break;

                MR - compute IG for appropriate non-leaf node

                update rules using MR result and checked rule
                    : previous - update rule - next
             */

            String tokens[] = nodeStr.split(conf.get(ArgumentsConstants.DELIMITER));
            conf.set(Constants.ID3_RULE_CONDITION, tokens[0]);
            conf.set(ArgumentsConstants.OUTPUT_PATH, outputBase + "/entropy_" + iterCnt);

            if(!computeAttributeEntropy(conf)) return 1;

            String oldRulePath = conf.get(ArgumentsConstants.RULE_PATH);
            conf.set(ArgumentsConstants.RULE_PATH, ruleFilePath + "_" + iterCnt);
            updateRule(conf, oldRulePath, nodeStr);

            iterCnt++;
        }
        logger.info("> ID3 Classification Iterations are Finished..");

        FileSystem.get(conf).rename(new Path(ruleFilePath + "_" + (iterCnt-1)), new Path(ruleFilePath));

        /**
        final classify: Map Only
            : file: input data, rule file
        Option: -finalResultGen
         */
        if(conf.get(ArgumentsConstants.FINAL_RESULT_GENERATION, "false").equals("true"))
        {
            /**
             * Final Classifying Result Generation
             * Data.... + Classifying Label
             *
             * Map Only Process
             */
            conf.set(ArgumentsConstants.OUTPUT_PATH, outputBase + "/classifying_result");
            conf.set(ArgumentsConstants.RULE_PATH, ruleFilePath);
            if(!finalClassifying(conf)) return 1;




            conf.set(ArgumentsConstants.INPUT_PATH, conf.get(ArgumentsConstants.OUTPUT_PATH));
            conf.set(ArgumentsConstants.OUTPUT_PATH, outputBase + "/validation_tmp");
            if(!confusionMatrixGen(conf)) return 1;

            ValidationMain validate = new ValidationMain();
            FileSystem fs = FileSystem.get(conf);
            validate.validationGeneration(fs,
                    conf.get(ArgumentsConstants.OUTPUT_PATH),
                    conf.get(ArgumentsConstants.DELIMITER),
                    outputBase + "/validation");

            /**
             *
             * 1. MR classifying Count
             *      1.1Map Output
             *          Original-Label, Classified-Label, 1
             *      2. Reduce Output (like WordCount)
             *          Original-Label, Classified-Label, Total Count
             *
             * 2. PostProcessing
             *      Reduce Result Read and Class Label Identification
             *      Make Result
             *          confusion Matrix
             *          summary > total instances, correctly classified instances, incorrect ~
             *          class, total(weighted) > TP rate, FP rate, Precision, Recall, F1-Measure
             */
        }

        if(conf.get(ArgumentsConstants.TEMP_DELETE, "true").equals("true"))
        {
            logger.info("> Temporary Files are Deleted..");
            for(int i=0; i<iterCnt-1; i++)
            {
                FileSystem.get(conf).delete(new Path(outputBase + "/entropy_" + i), true);
                FileSystem.get(conf).delete(new Path(ruleFilePath + "_" + i), true);
            }
            FileSystem.get(conf).delete(new Path(outputBase + "/entropy_" + (iterCnt-1)), true);
//            FileSystem.get(conf).delete(new Path(outputBase + "/classifying_result"), true);
            FileSystem.get(conf).delete(new Path(outputBase + "/validation_tmp"), true);

        }

        return 0;
    }


    /**
     * row data generation for confusion matrix (org-class, pred-class, frequency)
     * @param conf
     * @return
     * @throws Exception
     */
    private boolean confusionMatrixGen(Configuration conf) throws Exception
    {
        Job job = new Job(this.getConf());

        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.CLASS_INDEX, conf.get(ArgumentsConstants.CLASS_INDEX));

        job.setJarByClass(ID3Driver.class);

        job.setMapperClass(ConfusionMatrixMapper.class);
        job.setReducerClass(ConfusionMatrixReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.info("Error: ID3(Rutine) Final Validation Check is not Completeion");
            return false;
        }

        return true;
    }

    /**
     * classification result generation for train file (add class info to train data file)
     * @param conf
     * @return
     * @throws Exception
     */
    private boolean finalClassifying(Configuration conf) throws Exception
    {
        Job job = new Job(this.getConf());

        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.RULE_PATH, conf.get(ArgumentsConstants.RULE_PATH));

        job.setJarByClass(ID3Driver.class);

        job.setMapperClass(ID3FinalClassifyingMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        if(!job.waitForCompletion(true))
        {
            logger.info("Error: ID3(Rutine) Final Mapper is not Completeion");
            return false;
        }

        return true;
    }

    private String loadNonLeafNode(Configuration conf) throws Exception
    {
        String nodeStr = null;
        if(conf.get(ArgumentsConstants.RULE_PATH)==null) return "root";

        Path ruleFilePath = new Path(conf.get(ArgumentsConstants.RULE_PATH));
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream fin = fs.open(ruleFilePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));

        String readStr;
        br.readLine();
        while((readStr = br.readLine())!=null)
        {
            if((readStr.length() > 0) && readStr.substring(readStr.length() - 5).equals("false"))
            {
                nodeStr = readStr;
                break;
            }
        }

        br.close();
        fin.close();

        return nodeStr;
    }

    private void updateRule(Configuration conf, String oldRulePath, String ruleStr) throws Exception
    {
        // rule selecting
        RuleNodeBaseInfo[] nodes = getSelectedNodes(conf);

        // rule write
        writeRules(conf, nodes, conf.get(ArgumentsConstants.DELIMITER), oldRulePath, ruleStr);

    }

    private void writeRules(Configuration conf, RuleNodeBaseInfo[] nodes, String delimiter, String oldRulePath, String ruleStr) throws Exception
    {
        FileSystem fs = FileSystem.get(conf);

        if(ruleStr.equals("root"))
        {
            FSDataOutputStream fout = fs.create(new Path(conf.get(ArgumentsConstants.RULE_PATH)), true);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));

            bw.write("# [AttributeName-@@Attribute-Value][@@].., Data-Count, Node-Purity, Class-Label, Is-Leaf-Node" + "\n");

            for(int i=0; i<nodes.length; i++)
            {
                bw.write(nodes[i].toString(delimiter) + "\n");
            }

            bw.close();
            fout.close();
        }
        else
        {
            FSDataInputStream fin = FileSystem.get(conf).open(new Path(oldRulePath));
            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));

            FSDataOutputStream fout = fs.create(new Path(conf.get(ArgumentsConstants.RULE_PATH)), true);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));

            String readStr;
            while((readStr=br.readLine())!=null)
            {
                if(readStr.equals(ruleStr))
                {
                    if(nodes.length > 1)
                    {
                        bw.write(readStr + "-cont\n");
                        for(int i=0; i<nodes.length; i++)
                        {
                            bw.write(nodes[i].toString(delimiter) + "\n");
                        }
                    }
                    else bw.write(readStr + "-true\n");
                }
                else bw.write(readStr + "\n");
            }

            br.close();
            fin.close();

            bw.close();
            fout.close();
        }
    }


    private RuleNodeBaseInfo[] getSelectedNodes(Configuration conf) throws Exception
    {
        String delimiter = conf.get(ArgumentsConstants.DELIMITER);
        Path entropyPath = new Path(conf.get(ArgumentsConstants.OUTPUT_PATH));
        FileStatus[] status = FileSystem.get(conf).listStatus(entropyPath);

        String selectedStr = "";
        double minEntropy = 9999.0;
        for(int i=0; i<status.length; i++)
        {
            if(!status[i].getPath().toString().contains("part-r-")) continue;

            FSDataInputStream fin = FileSystem.get(conf).open(status[i].getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
            String readStr;
            while((readStr = br.readLine())!=null)
            {
                double curEntropy = Double.parseDouble(readStr.split(delimiter)[1]);
                if(minEntropy > curEntropy)
                {
                    minEntropy = curEntropy;
                    selectedStr = readStr;
                }
            }
            br.close();
            fin.close();
        }

        String tokens[] = selectedStr.split(delimiter);
        int attrCnt = (tokens.length - 2) / 4;    // index, entropy and 4-set
        RuleNodeBaseInfo[] retNodes = new RuleNodeBaseInfo[attrCnt];
        int minDataCnt = Integer.parseInt(conf.get(ArgumentsConstants.MIN_LEAF_DATA));
        double minPurity = Double.parseDouble(conf.get(ArgumentsConstants.PURITY));

        for(int i=0; i<attrCnt; i++)
        {
            int base = i * 4;
            int dataCnt = Integer.parseInt(tokens[base+3]);
            double purity = Double.parseDouble(tokens[base+4]);

            retNodes[i] = new RuleNodeBaseInfo(tokens[0]
                    , tokens[base+2]
                    , dataCnt, purity
                    , tokens[base+5]);

            if((minDataCnt >= dataCnt) ||(minPurity < purity)) retNodes[i].setIsLeaf(true);
            else retNodes[i].setIsLeaf(false);
        }

        return retNodes;
    }

    private boolean computeAttributeEntropy(Configuration conf) throws Exception
    {
        Job job = new Job(this.getConf());

        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.NUMERIC_INDEX, conf.get(ArgumentsConstants.NUMERIC_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.CLASS_INDEX, conf.get(ArgumentsConstants.CLASS_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.MIN_LEAF_DATA, conf.get(ArgumentsConstants.MIN_LEAF_DATA, "1"));
        job.getConfiguration().set(ArgumentsConstants.PURITY, conf.get(ArgumentsConstants.PURITY, "1"));
        job.getConfiguration().set(Constants.ID3_RULE_CONDITION, conf.get(Constants.ID3_RULE_CONDITION, "root"));

        job.setJarByClass(ID3Driver.class);
        job.setMapperClass(ID3AttributeSplitMapper.class);
        job.setReducerClass(ID3ComputeEntropyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.info("Error: 1-MR for ID3(Rutine) is not Completeion");
            return false;
        }

        return true;
    }


    public static void main(String args[]) throws Exception
    {
        int res = ToolRunner.run(new ID3Driver(), args);
        System.exit(res);
    }
}
