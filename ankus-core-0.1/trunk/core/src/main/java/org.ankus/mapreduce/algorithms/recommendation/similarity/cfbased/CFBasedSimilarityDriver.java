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
package org.ankus.mapreduce.algorithms.recommendation.similarity.cfbased;

import org.ankus.io.TextDoublePairWritableComparable;
import org.ankus.io.TextDoubleTwoPairsWritableComparable;
import org.ankus.io.TextTwoWritableComparable;
import org.ankus.util.AnkusUtils;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.ankus.util.Usage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * CFBasedSimilarityDriver
 * @desc
 *      User-based Collaborative Filtering recommendation algorithms
 *      1. Cosine coefficient 2. Pearson coefficient
 *      Required data set : [userID, itemID, rating]
 * @version 0.0.1
 * @date : 2013.07.20
 * @author Suhyun Jeon
 */
public class CFBasedSimilarityDriver extends Configured implements Tool {

    private String input = null;
    private String output = null;
    private String basedType = null;
    private String algorithmOption = null;
    private String threshold = null;
    private String delimiter = null;
    // Default value is 10
    private String commonCount = "10";
    private FileSystem fileSystem = null;

    // SLF4J Logging
    private Logger logger = LoggerFactory.getLogger(CFBasedSimilarityDriver.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new CFBasedSimilarityDriver(), args);
        System.exit(res);
    }

	@Override
    public int run(String[] args) throws Exception {

        if(args.length < 1){
            Usage.printUsage(Constants.ALGORITHM_COLLABORATIVE_FILTERING_BASED_SIMILARITY);
            return -1;
        }

        initArguments(args);

        // Get prepare output path for in the middle of job processing
        String prepareDirectory = AnkusUtils.createDirectoryForHDFS(output);
        String prepareOutput = prepareDirectory + "/" + algorithmOption + "/";
        fileSystem = FileSystem.get(new Configuration());

        URI fileSystemUri = fileSystem.getUri();
        Path prepareOutputPath = new Path(fileSystemUri + "/" + prepareOutput);

        logger.info("==========================================================================================");
        logger.info("Prepare output directory is [" + prepareOutputPath.toString() + "]");
        logger.info("==========================================================================================");

    	Job job1 = new Job();           
        job1.setJarByClass(CFBasedSimilarityDriver.class);
       
        job1.setMapperClass(CFBasedSimilarityMapper.class);
        job1.setReducerClass(CFBasedSimilarityReducer.class);
        
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(TextDoublePairWritableComparable.class);
        
        job1.setOutputKeyClass(TextTwoWritableComparable.class);
        job1.setOutputValueClass(TextDoubleTwoPairsWritableComparable.class);

        FileInputFormat.setInputPaths(job1, new Path(input));
		FileOutputFormat.setOutputPath(job1, prepareOutputPath);
		job1.getConfiguration().set(Constants.BASED_TYPE, basedType);
		job1.getConfiguration().set(Constants.DELIMITER, delimiter);
       
        boolean step1 = job1.waitForCompletion(true);
        if(!(step1)) return -1;
        
        
        Job job2 = new Job();           
        job2.setJarByClass(CFBasedSimilarityDriver.class);
       
        job2.setMapperClass(CalculationCFBasedSimilarityMapper.class);
        job2.setReducerClass(CalculationCFBasedSimilarityReducer.class);
        
        job2.setMapOutputKeyClass(TextTwoWritableComparable.class);
        job2.setMapOutputValueClass(TextDoubleTwoPairsWritableComparable.class);
        
        job2.setOutputKeyClass(TextTwoWritableComparable.class);
        job2.setOutputValueClass(TextDoubleTwoPairsWritableComparable.class);
        
        FileInputFormat.setInputPaths(job2, prepareOutputPath);
        FileOutputFormat.setOutputPath(job2, new Path(output));

        job2.getConfiguration().set(Constants.DELIMITER, delimiter);
        job2.getConfiguration().set(Constants.ALGORITHM_OPTION, algorithmOption);
        job2.getConfiguration().set(Constants.THRESHOLD, threshold);
        job2.getConfiguration().set(Constants.COMMON_COUNT, commonCount);

        boolean step2 = job2.waitForCompletion(true);
        if(!(step2)) return -1;
        
        return 0;
    }

    private void initArguments(String[] args) {
        try{
            for (int i = 0; i < args.length; ++i) {
                if (ArgumentsConstants.INPUT_PATH.equals(args[i])) {
                    input = args[++i];
                } else if (ArgumentsConstants.OUTPUT_PATH.equals(args[i])) {
                    output = args[++i];
                } else if (ArgumentsConstants.BASED_TYPE.equals(args[i])) {
                    basedType = args[++i];
                } else if (ArgumentsConstants.ALGORITHM_OPTION.equals(args[i])) {
                    algorithmOption = args[++i];
                } else if (ArgumentsConstants.DELIMITER.equals(args[i])) {
                    delimiter = args[++i];
                } else if (ArgumentsConstants.THRESHOLD.equals(args[i])) {
                    threshold = args[++i];
                } else if (ArgumentsConstants.COMMON_COUNT.equals(args[i])) {
                    commonCount = args[++i];
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}