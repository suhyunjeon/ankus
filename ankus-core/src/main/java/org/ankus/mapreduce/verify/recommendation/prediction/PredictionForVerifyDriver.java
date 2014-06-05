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
package org.ankus.mapreduce.verify.recommendation.prediction;

import org.ankus.mapreduce.algorithms.recommendation.recommender.itemlist.ItemListMapper;
import org.ankus.mapreduce.algorithms.recommendation.recommender.itemlist.ItemListReducer;
import org.ankus.mapreduce.algorithms.recommendation.recommender.neighborhood.aggregate.*;
import org.ankus.mapreduce.algorithms.recommendation.recommender.neighborhood.userbased.UserbasedMovielensMapper;
import org.ankus.mapreduce.algorithms.recommendation.recommender.neighborhood.userbased.UserbasedNeighborhoodMapper;
import org.ankus.mapreduce.algorithms.recommendation.recommender.neighborhood.userbased.UserbasedNeighborhoodReducer;
import org.ankus.mapreduce.algorithms.recommendation.recommender.prediction.PredictionItemsMapper;
import org.ankus.mapreduce.algorithms.recommendation.recommender.prediction.RecommendationReducer;
import org.ankus.mapreduce.algorithms.recommendation.recommender.prediction.SimilarityDataMapper;
import org.ankus.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;

/**
 * PredictionForVerifyDriver
 * @desc
 *      Calculating rmse by ankus recommendation system.
 *      This is rearrange recommendation system. There is no candidate step.
 *      Only support User-based Collaborative Filtering recommendation algorithms
 *      Runs a recommendation job as a series of map/reduce
 * @version 0.1
 * @date : 2013.11.18
 * @author Suhyun Jeon
 */
public class PredictionForVerifyDriver extends Configured implements Tool {

    private String input = null;
    private String similarDataInput = null;
    private String output = null;
    private String delimiter = null;
    private FileSystem fileSystem = null;

    // SLF4J Logging
    private Logger logger = LoggerFactory.getLogger(PredictionForVerifyDriver.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new PredictionForVerifyDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        if(args.length < 1){
            Usage.printUsage(Constants.ALGORITHM_RECOMMENDATION);
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
        String outputURI = fileSystemUri + "/" + prepareOutput;
        Path itemListOutputPath = new Path(outputURI + "itemlist");
        Path candidateItemListOutput = new Path(outputURI + "candidate");
        Path switchSimilarityOutput1 = new Path(outputURI + "switchSimilarity1");
        Path switchSimilarityOutput2 = new Path(outputURI + "switchSimilarity2");
        Path aggregateSwitchSimOutput = new Path(outputURI + "aggregate");
        Path neighborAllDataOutput = new Path(outputURI + "neighborhood");

        /**
         * Step 1.
         * Arrange only item list of test data set(base input data set)
         */
        logger.info("==========================================================================================");
        logger.info("   Step 1 of the 7 steps : Arrange only item list for input data set. ");
        logger.info("       Input directory [" + input + "]");
        logger.info("       Output directory [" + itemListOutputPath.toString() + "]");
        logger.info("==========================================================================================");

        Job job1 = new Job(this.getConf());
        job1 = HadoopUtil.prepareJob(job1, new Path(input), itemListOutputPath, PredictionForVerifyDriver.class,
                ItemListMapper.class, Text.class, NullWritable.class,
                ItemListReducer.class, Text.class, NullWritable.class);

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

        Job job2 = new Job(this.getConf());
        job2 = HadoopUtil.prepareJob(job2, new Path(similarDataInput), switchSimilarityOutput1, PredictionForVerifyDriver.class,
                Neighborhood1Mapper.class, Text.class, Text.class,
                Neighborhood1Reducer.class, Text.class, Text.class);

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

        Job job3 = new Job(this.getConf());
        job3 = HadoopUtil.prepareJob(job3, new Path(similarDataInput), switchSimilarityOutput2, PredictionForVerifyDriver.class,
                Neighborhood2Mapper.class, Text.class, Text.class,
                Neighborhood2Reducer.class, Text.class, Text.class);

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

        Job job4 = new Job(this.getConf());
        job4 = HadoopUtil.prepareJob(job4, switchSimilarityOutput1, switchSimilarityOutput2, aggregateSwitchSimOutput, PredictionForVerifyDriver.class,
                AggregateMapper.class, NullWritable.class, Text.class);

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

        Job job5 = new Job(this.getConf());
        job5 = HadoopUtil.prepareJob(job5, new Path(input), aggregateSwitchSimOutput, neighborAllDataOutput, PredictionForVerifyDriver.class,
                UserbasedMovielensMapper.class, UserbasedNeighborhoodMapper.class, Text.class, Text.class,
                UserbasedNeighborhoodReducer.class, Text.class, Text.class);

        job5.getConfiguration().set(Constants.DELIMITER, delimiter);

        boolean step5 = job5.waitForCompletion(true);
        if(!(step5)) return -1;


        /**
         * Step 4.
         * Finally calculator candidate rating of n users
         */
        logger.info("==========================================================================================");
        logger.info("   Step 7 of the 7 steps : Finally calculator candidate rating of n users. ");
        logger.info("       Multi Input directory 1 [" + candidateItemListOutput.toString() + "]");
        logger.info("       Multi Input directory 2 [" + neighborAllDataOutput.toString() + "]");
        logger.info("       Output directory [" + output + "]");
        logger.info("==========================================================================================");

        Job job7 = new Job(this.getConf());
        job7 = HadoopUtil.prepareJob(job7, new Path(input), neighborAllDataOutput, new Path(output), PredictionForVerifyDriver.class,
                PredictionItemsMapper.class, SimilarityDataMapper.class, Text.class, Text.class,
                RecommendationReducer.class, Text.class, DoubleWritable.class);

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