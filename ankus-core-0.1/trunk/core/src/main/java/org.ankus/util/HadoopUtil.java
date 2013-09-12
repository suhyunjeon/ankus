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

/**
 * HadoopUtil
 * @desc
 *      Create a map/reduce Hadoop job. Referenced the Apache Mahout.
 * @return
 *      Job
 * @version 0.0.1
 * @date : 2013.09.09
 * @author Suhyun Jeon
 */
package org.ankus.util;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class HadoopUtil {

    private static final Logger log = LoggerFactory.getLogger(HadoopUtil.class);

    private HadoopUtil() { }

    /**
     * Create a map and reduce Hadoop job.
     * @param inputPath The input {@link org.apache.hadoop.fs.Path}
     * @param outputPath The output {@link org.apache.hadoop.fs.Path}
     * @param inputFormat The {@link org.apache.hadoop.mapreduce.InputFormat}
     * @param mapper The {@link org.apache.hadoop.mapreduce.Mapper} class to use
     * @param mapperKey The {@link org.apache.hadoop.io.Writable} key class.  If the Mapper is a no-op, this value may be null
     * @param mapperValue The {@link org.apache.hadoop.io.Writable} value class.  If the Mapper is a no-op, this value may be null
     * @param reducer The {@link org.apache.hadoop.mapreduce.Reducer} to use
     * @param reducerKey The reducer key class.
     * @param reducerValue The reducer value class.
     * @param outputFormat The {@link org.apache.hadoop.mapreduce.OutputFormat}.
     * @param conf The {@link org.apache.hadoop.conf.Configuration} to use.
     * @return The {@link org.apache.hadoop.mapreduce.Job}.
     * @throws IOException if there is a problem with the IO.
     *
     * @see #getCustomJobName(String, org.apache.hadoop.mapreduce.JobContext, Class, Class)
     * @see #prepareJob(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path, Class, Class, Class, Class, Class, org.apache.hadoop.conf.Configuration)
     */

//    Job job1 = new Job();
//    job1.setJarByClass(BooleanSetDriver.class);
//
//    job1.setMapperClass(BooleanSetMapper.class);
//    job1.setReducerClass(BooleanSetReducer.class);
//
//    job1.setMapOutputKeyClass(Text.class);
//    job1.setMapOutputValueClass(TextIntegerPairWritableComparable.class);
//
//    job1.setOutputKeyClass(TextTwoWritableComparable.class);
//    job1.setOutputValueClass(TextTwoWritableComparable.class);
//
//    FileInputFormat.setInputPaths(job1, new Path(input));
//    FileOutputFormat.setOutputPath(job1, prepareOutputPath);
//
//    job1.getConfiguration().set(Constants.DELIMITER, delimiter);
//    job1.getConfiguration().set(Constants.KEY_INDEX, keyIndex);

    public static Job prepareJob(Path inputPath,
                                 Path outputPath,
                                 Class<? extends Configured> driver,
                                 Class<? extends Mapper> mapper,
                                 Class<? extends Writable> mapperKey,
                                 Class<? extends Writable> mapperValue,
                                 Class<? extends Reducer> reducer,
                                 Class<? extends Writable> reducerKey,
                                 Class<? extends Writable> reducerValue) throws IOException {

        Job job = new Job();
        job.setJarByClass(driver);

        job.setMapperClass(mapper);
        job.setReducerClass(reducer);

        job.setMapOutputKeyClass(mapperKey);
        job.setMapOutputValueClass(mapperValue);

        job.setOutputKeyClass(reducerKey);
        job.setOutputValueClass(reducerValue);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }
}

