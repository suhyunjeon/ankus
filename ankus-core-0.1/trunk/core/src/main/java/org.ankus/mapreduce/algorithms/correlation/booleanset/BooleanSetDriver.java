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
package org.ankus.mapreduce.algorithms.correlation.booleanset;

import org.ankus.io.*;
import org.ankus.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;

/**
 * BooleanSetDriver
 * @desc
 *      Here's an version of the similarity coefficient and distance calculation.
 *      1. Dice coefficient 2. Jaccard coefficient 3. Hamming distance
 *
 * Example dataset
 * ------------------------
 * 1    0   1   1   1   0
 * 0    0   0   0   1   1
 * 1    0   1   0   1   0
 *
 * @return The is between the two input VECTOR boolean data set.
 * 		   Returns 1 if one 0 or both of the boolean are not {@code 0 or 1}.
 *
 * @version 0.0.1
 * @date : 2013.07.10
 * @author Suhyun Jeon
*/
public class BooleanSetDriver extends Configured implements Tool {

    // Output arguments
    private String input = null;
    private String output = null;
    private String keyIndex = null;
    private String algorithmOption = null;
    private String delimiter = null;
    private FileSystem fileSystem = null;

    // SLF4J Logging
    private Logger logger = LoggerFactory.getLogger(BooleanSetDriver.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new BooleanSetDriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        if(args.length < 1){
            Usage.printUsage(Constants.ALGORITHM_BOOLEAN_DATA_CORRELATION);
            return -1;
        }

        initArguments(args);

        // Get key (midterm.process.output.remove.mode) from config.properties
        Properties configProperties = AnkusUtils.getConfigProperties();
        String removeMidtermProcess = configProperties.get(Constants.MIDTERM_PROCESS_OUTPUT_REMOVE_MODE).toString();
        boolean removeMode = false;
        if(removeMidtermProcess.equals(Constants.REMOVE_ON)){
            removeMode = true;
        }

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
        job1.setJarByClass(BooleanSetDriver.class);

        job1.setMapperClass(BooleanSetMapper.class);
        job1.setReducerClass(BooleanSetReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(TextIntegerPairWritableComparable.class);

        job1.setOutputKeyClass(TextTwoWritableComparable.class);
        job1.setOutputValueClass(TextIntegerTwoPairsWritableComparable.class);

		FileInputFormat.setInputPaths(job1, new Path(input));
		FileOutputFormat.setOutputPath(job1, prepareOutputPath);

		job1.getConfiguration().set(Constants.DELIMITER, delimiter);
        job1.getConfiguration().set(Constants.KEY_INDEX, keyIndex);

        boolean step1 = job1.waitForCompletion(true);
        if(!(step1)) return -1;


        Job job2 = new Job();
        job2.setJarByClass(BooleanSetDriver.class);

        job2.setMapperClass(CalculationBooleanSetMapper.class);
        job2.setReducerClass(CalculationBooleanSetReducer.class);

        job2.setMapOutputKeyClass(TextTwoWritableComparable.class);
        job2.setMapOutputValueClass(TextIntegerTwoPairsWritableComparable.class);

        job2.setOutputKeyClass(TextTwoWritableComparable.class);
        job2.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.setInputPaths(job2, prepareOutputPath);
        FileOutputFormat.setOutputPath(job2, new Path(output));

        job2.getConfiguration().set(Constants.DELIMITER, delimiter);
        job2.getConfiguration().set(Constants.ALGORITHM_OPTION, algorithmOption);

        boolean step2 = job2.waitForCompletion(true);
        if(!(step2)) return -1;

        // Remove all midterm process output files.
        if(removeMode){
            boolean delete = fileSystem.delete(prepareOutputPath, true);
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
                } else if (ArgumentsConstants.OUTPUT_PATH.equals(args[i])) {
                    output = args[++i];
                } else if (ArgumentsConstants.KEY_INDEX.equals(args[i])) {
                    keyIndex = args[++i];
                } else if (ArgumentsConstants.ALGORITHM_OPTION.equals(args[i])) {
                    algorithmOption = args[++i];
                } else if (ArgumentsConstants.DELIMITER.equals(args[i])) {
                    delimiter = args[++i];
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}