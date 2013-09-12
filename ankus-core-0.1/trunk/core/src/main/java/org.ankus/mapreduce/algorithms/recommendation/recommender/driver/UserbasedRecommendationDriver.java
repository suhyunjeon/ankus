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
package org.ankus.mapreduce.algorithms.recommendation.recommender.driver;

import org.ankus.io.TextTwoWritableComparable;
import org.ankus.mapreduce.algorithms.recommendation.recommender.prediction.PredictionMapper;
import org.ankus.mapreduce.algorithms.recommendation.recommender.prediction.PredictionReducer;
import org.ankus.mapreduce.algorithms.recommendation.recommender.itemlist.ItemListMapper;
import org.ankus.mapreduce.algorithms.recommendation.recommender.itemlist.ItemListReducer;
import org.ankus.mapreduce.algorithms.recommendation.recommender.neighborhood.userbased.MovielensMapper;
import org.ankus.mapreduce.algorithms.recommendation.recommender.neighborhood.userbased.NeighborhoodMapper;
import org.ankus.mapreduce.algorithms.recommendation.recommender.neighborhood.userbased.NeighborhoodReducer;
import org.ankus.mapreduce.algorithms.recommendation.recommender.neighborhood.aggregate.*;
import org.ankus.mapreduce.algorithms.recommendation.recommender.recommendation.userbased.PredictionItemsMapper;
import org.ankus.mapreduce.algorithms.recommendation.recommender.recommendation.userbased.RecommendationReducer;
import org.ankus.mapreduce.algorithms.recommendation.recommender.recommendation.userbased.UserSimilarityMapper;
import org.ankus.util.AnkusUtils;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.ankus.util.Usage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;

/**
 * UserbasedRecommendationDriver
 * @desc
 *      User-based Collaborative Filtering recommendation algorithms
 *      Runs a recommendation job as a series of map/reduce
 * @version 0.0.1
 * @date : 2013.07.13
 * @author Suhyun Jeon
*/
public class UserbasedRecommendationDriver extends Configured implements Tool {

    private String input = null;
    private String similarDataInput = null;
    private String output = null;
    private String delimiter = null;

    private FileSystem fileSystem = null;

    // SLF4J Logging
    private Logger logger = LoggerFactory.getLogger(UserbasedRecommendationDriver.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new UserbasedRecommendationDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        if(args.length < 1){
            Usage.printUsage(Constants.ALGORITHM_USER_BASED_RECOMMENDATION);
            return -1;
        }

        initArguments(args);

        // Get key (midterm.process.output.remove.mode) from config.properties
        Properties configProperties = AnkusUtils.getConfigProperties();
        String removeModeMidtermProcess = configProperties.get(Constants.MIDTERM_PROCESS_OUTPUT_REMOVE_MODE).toString();
        boolean removeMode = false;
        if(removeModeMidtermProcess.equals(Constants.REMOVE_ON)){
            removeMode = true;
        }

        // Get prepare output path for in the middle of job processing
        String prepareDirectory = AnkusUtils.createDirectoryForHDFS(output);
        String prepareOutput = prepareDirectory + "/";
        fileSystem = FileSystem.get(new Configuration());

        URI fileSystemUri = fileSystem.getUri();
        Path itemListOutputPath = new Path(fileSystemUri + "/" + prepareOutput + "itemlist");
        Path candidateItemListOutput = new Path(fileSystemUri + "/" + prepareOutput + "candidate");
        Path switchSimilarityOutput1 = new Path(fileSystemUri + "/" + prepareOutput + "switchSimilarity1");
        Path switchSimilarityOutput2 = new Path(fileSystemUri + "/" + prepareOutput + "switchSimilarity2");
        Path aggregateSwitchSimOutput = new Path(fileSystemUri + "/" + prepareOutput + "aggregate");
        Path neighborAllDataOutput = new Path(fileSystemUri + "/" + prepareOutput + "neighborhood");

        /**
         * Step 1.
         * Arrange only item list of test data set(base input data set)
         */
        logger.info("==========================================================================================");
        logger.info("   Step 1 of the 7 steps : Arrange only item list for input data set. ");
        logger.info("       Input directory [" + input + "]");
        logger.info("       Output directory [" + itemListOutputPath.toString() + "]");
        logger.info("==========================================================================================");

        Job job1 = new Job();
        job1.setJarByClass(UserbasedRecommendationDriver.class);

        job1.setMapperClass(ItemListMapper.class);
        job1.setReducerClass(ItemListReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(NullWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, itemListOutputPath);

        job1.getConfiguration().set(Constants.DELIMITER, delimiter);

        boolean step1 = job1.waitForCompletion(true);
        if(!(step1)) return -1;


        /**
         * Step 1-1.
         * Arrange similar users from similarity data set and movielens data set
         */
        logger.info("==========================================================================================");
        logger.info("   Step 2 of the 7 steps : Arrange similar users from similarity data set and movielens data set. ");
        logger.info("       Input directory [" + similarDataInput + "]");
        logger.info("       Output directory [" + switchSimilarityOutput1.toString() + "]");
        logger.info("==========================================================================================");

        Job job2 = new Job();
        job2.setJarByClass(UserbasedRecommendationDriver.class);

        job2.setMapperClass(Neighborhood1Mapper.class);
        job2.setReducerClass(Neighborhood1Reducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job2, similarDataInput);
        FileOutputFormat.setOutputPath(job2, switchSimilarityOutput1);

        job2.getConfiguration().set(Constants.DELIMITER, delimiter);

        boolean step2 = job2.waitForCompletion(true);
        if(!(step2)) return -1;

        /**
         * Step 1-2.
         * Opposite arrange similar users from similarity data set and movielens data set
         */
        logger.info("==========================================================================================");
        logger.info("   Step 3 of the 7 steps : Opposite arrange similar users from similarity data set and movielens data set. ");
        logger.info("       Input directory [" + similarDataInput + "]");
        logger.info("       Output directory [" + switchSimilarityOutput2.toString() + "]");
        logger.info("==========================================================================================");

        Job job3 = new Job();
        job3.setJarByClass(UserbasedRecommendationDriver.class);

        job3.setMapperClass(Neighborhood2Mapper.class);
        job3.setReducerClass(Neighborhood2Reducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job3, similarDataInput);
        FileOutputFormat.setOutputPath(job3, switchSimilarityOutput2);

        job3.getConfiguration().set(Constants.DELIMITER, delimiter);

        boolean step3 = job3.waitForCompletion(true);
        if(!(step3)) return -1;

        /**
         * 1-3 Aggregate two similarity result data set
         */
        logger.info("==========================================================================================");
        logger.info("   Step 4 of the 7 steps : Aggregate step 2 and step 3 result data set. ");
        logger.info("       Multi Input directory 1 [" + switchSimilarityOutput1 + "]");
        logger.info("       Multi Input directory 2 [" + switchSimilarityOutput2 + "]");
        logger.info("       Output directory [" + aggregateSwitchSimOutput.toString() + "]");
        logger.info("==========================================================================================");

        Job job4 = new Job();
        job4.setJarByClass(UserbasedRecommendationDriver.class);

        job4.setMapperClass(AggregateMapper.class);

        job4.setMapOutputKeyClass(NullWritable.class);
        job4.setMapOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job4, switchSimilarityOutput1, TextInputFormat.class);
        MultipleInputs.addInputPath(job4, switchSimilarityOutput2, TextInputFormat.class);
        FileOutputFormat.setOutputPath(job4, aggregateSwitchSimOutput);

        boolean step4 = job4.waitForCompletion(true);
        if(!(step4)) return -1;

        /**
         * Step 2.
         * Join movielens data set and similarity(neighborhood) user list
         */
        logger.info("==========================================================================================");
        logger.info("   Step 5 of the 7 steps : Join movielens data set and similarity(step 4) user list. ");
        logger.info("       Multi Input directory 1 [" + input + "]");
        logger.info("       Multi Input directory 2 [" + aggregateSwitchSimOutput.toString() + "]");
        logger.info("       Output directory [" + neighborAllDataOutput.toString() + "]");
        logger.info("==========================================================================================");

        Job job5 = new Job();
        job5.setJarByClass(UserbasedRecommendationDriver.class);

        job5.setReducerClass(NeighborhoodReducer.class);

        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);

        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job5, new Path(input), TextInputFormat.class, MovielensMapper.class);
        MultipleInputs.addInputPath(job5, aggregateSwitchSimOutput, TextInputFormat.class, NeighborhoodMapper.class);
        FileOutputFormat.setOutputPath(job5, neighborAllDataOutput);

        job5.getConfiguration().set(Constants.DELIMITER, delimiter);

        boolean step5 = job5.waitForCompletion(true);
        if(!(step5)) return -1;

        /**
         * Step 3.
         * Arrange prediction items for n users
         */
        logger.info("==========================================================================================");
        logger.info("   Step 6 of the 7 steps : Arrange prediction items for n users. ");
        logger.info("       Input directory [" + input + "]");
        logger.info("       Input directory to setup method [" + itemListOutputPath.toString() + "]");
        logger.info("       Output directory [" + candidateItemListOutput.toString() + "]");
        logger.info("==========================================================================================");

        Job job6 = new Job();
        job6.setJarByClass(UserbasedRecommendationDriver.class);

        job6.setMapperClass(PredictionMapper.class);
        job6.setReducerClass(PredictionReducer.class);

        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(TextTwoWritableComparable.class);

        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job6, new Path(input));
        FileOutputFormat.setOutputPath(job6, candidateItemListOutput);

        job6.getConfiguration().set(Constants.DELIMITER, delimiter);
        job6.getConfiguration().set("itemListPath", itemListOutputPath.toString());

        boolean step6 = job6.waitForCompletion(true);
        if(!(step6)) return -1;

        /**
         * Step 4.
         * Finally calculator prediction rating of n users
         */
        logger.info("==========================================================================================");
        logger.info("   Step 7 of the 7 steps : Finally calculator prediction rating of n users. ");
        logger.info("       Multi Input directory 1 [" + candidateItemListOutput.toString() + "]");
        logger.info("       Multi Input directory 2 [" + neighborAllDataOutput.toString() + "]");
        logger.info("       Output directory [" + output + "]");
        logger.info("==========================================================================================");

        Job job7 = new Job();
        job7.setJarByClass(UserbasedRecommendationDriver.class);

        job7.setReducerClass(RecommendationReducer.class);

        job7.setMapOutputKeyClass(Text.class);
        job7.setMapOutputValueClass(Text.class);

        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job7, candidateItemListOutput, TextInputFormat.class, PredictionItemsMapper.class);
        MultipleInputs.addInputPath(job7, neighborAllDataOutput, TextInputFormat.class, UserSimilarityMapper.class);
        FileOutputFormat.setOutputPath(job7, new Path(output));

        job7.getConfiguration().set(Constants.DELIMITER, delimiter);

        boolean step7 = job7.waitForCompletion(true);
        if(!(step7)) return -1;

        // Remove all midterm process output files.
        if(removeMode){
            boolean delete = fileSystem.delete(new Path(fileSystemUri + "/" + prepareOutput), true);
            if(delete){
                logger.info("Delete midterm process output files.");
            }
        }
        return 0;
    }

    private void initArguments(String[] args) {
        try{
            for (int i = 0; i < args.length; ++i) {
                if (ArgumentsConstants.INPUT_PATH.equals(args[i])) {
                    input = args[++i];
                } else if (ArgumentsConstants.SIMILARITY_DATA_INPUT.equals(args[i])) {
                    similarDataInput = args[++i];
                } else if (ArgumentsConstants.OUTPUT_PATH.equals(args[i])) {
                    output = args[++i];
                } else if (ArgumentsConstants.DELIMITER.equals(args[i])) {
                    delimiter = args[++i];
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}