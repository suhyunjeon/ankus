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
package org.ankus.mapreduce.algorithms.preprocessing.normalize;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.ankus.mapreduce.algorithms.statistics.numericstats.NumericStatsDriver;
import org.ankus.util.ConfigurationVariable;
import org.ankus.util.Constants;
import org.ankus.util.Usage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.ankus.util.ArgumentsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NormalizeDriver
 * @desc
 *  Min/Max based Normalization for Numeric Features
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class NormalizeDriver extends Configured implements Tool {

    private Logger logger = LoggerFactory.getLogger(NormalizeDriver.class);

	@Override
	public int run(String[] args) throws Exception
	{
        logger.info("Normalization MR-Job is Started..");
		
		Configuration conf = new Configuration();
		if(!ConfigurationVariable.setFromArguments(args, conf))
		{
            Usage.printUsage(Constants.ALGORITHM_NORMALIZE);
            logger.info("Error: MR Job Setting Failed..: Configuration Failed");
            return 1;
		}

        logger.info("Numeric Statistics for getting Min/Max value MR-Job is started..");
		
		String statJobOutput = conf.get(ArgumentsConstants.OUTPUT_PATH, null) + "_numericStat";
		String argsForStat[] = getParametersForStatJob(conf, statJobOutput);
		
		NumericStatsDriver statsJob = new NumericStatsDriver();
		if(statsJob.run(argsForStat)!=0)
        {
            logger.info("Numeric Statistics for getting Min/Max value MR-Job is Failed..");
            return 1;
        }

        logger.info("Numeric Statistics for getting Min/Max value MR-Job is successfully finished..");
        logger.info("Normalization Step of MR-Job is Started..");

        Job job = new Job();        
        setJob(job, conf);
        setMinMax(job.getConfiguration(), statJobOutput);
        
        job.setJarByClass(NormalizeDriver.class);

        // Mapper & Reducer Class
        job.setMapperClass(NormalizeMapper.class);        

        // Mapper Output Key & Value Type after Hadoop 0.20
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Reducer Output Key & Value Type
        job.setNumReduceTasks(0);

        if(!job.waitForCompletion(true))
    	{
        	logger.error("Error: MR for Normalization is not Completion");
            logger.info("MR-Job is Failed..");
        	return 1;
        }
        
        if(conf.get(ArgumentsConstants.TEMP_DELETE, "true").equals("true"))
        {
            logger.info("Temporary Files are Deleted..: " + statJobOutput);
        	FileSystem.get(conf).delete(new Path(statJobOutput), true);
        }

        logger.info("Normalization Step of MR-Job is Successfully Finished...");
        return 0;
        
	}

    /**
     * @desc set min/max value to configuration from file(numeric stat mr job)
     * @parameter
     *      conf : configuration identifier for job
     *      outputDirPath : output(result) path of numeric stat mr job
     */
	private void setMinMax(Configuration conf, String outputDirPath) throws Exception
	{	
		String delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(new Path(outputDirPath));
        for (int i=0;i<status.length;i++)
        {
        	BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath()), Constants.UTF8));
        	String readStr, tokens[];
        	while((readStr=br.readLine())!=null)
        	{
        		tokens = readStr.split(delimiter);
        		conf.set(ConfigurationVariable.MINMAX_VALUE + "_" + tokens[0], tokens[8] + "," + tokens[7]);
        	}
        	br.close();
        }
	}

    /**
     * @desc parameter(arguments) setting for numeric stat mr job execution (for get min/max value)
     * @parameter
     *      conf : configuration identifier for job
     *      outputPath : output path of numeric stat mr job
     * @return string array typed parameters(arguments)
     */
	private String[] getParametersForStatJob(Configuration conf, String outputPath) throws Exception 
	{
		String params[] = new String[14];
		
		params[0] = ArgumentsConstants.INPUT_PATH;
		params[1] = conf.get(ArgumentsConstants.INPUT_PATH, null);
		
		params[2] = ArgumentsConstants.OUTPUT_PATH;
		params[3] = outputPath;
		
		params[4] = ArgumentsConstants.DELIMITER;
		params[5] = conf.get(ArgumentsConstants.DELIMITER, "\t");
		
		params[6] = ArgumentsConstants.TARGET_INDEX;
		params[7] = conf.get(ArgumentsConstants.TARGET_INDEX, "-1");
		
		params[8] = ArgumentsConstants.EXCEPTION_INDEX;
		params[9] = conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1");
		
		params[10] = ArgumentsConstants.TEMP_DELETE;
		params[11] = conf.get(ArgumentsConstants.TEMP_DELETE, "true");
		
		params[12] = ArgumentsConstants.MR_JOB_STEP;
		params[13] = conf.get(ArgumentsConstants.MR_JOB_STEP, "1");
		
		return params;
	}
	
	public static void main(String args[]) throws Exception 
	{
		int res = ToolRunner.run(new NormalizeDriver(), args);
        System.exit(res);
	}

    /**
     * @desc configuration setting for mr job
     * @parameter
     *      job : job identifier
     *      conf : configuration identifier for job
     */
	private void setJob(Job job, Configuration conf) throws IOException 
	{
		FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
		job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
		job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
		job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
		job.getConfiguration().set(ArgumentsConstants.REMAIN_FIELDS, conf.get(ArgumentsConstants.REMAIN_FIELDS, "true"));
	}
}
