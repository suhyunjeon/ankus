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
package org.ankus.mapreduce.algorithms.clustering.kmeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.ankus.mapreduce.algorithms.preprocessing.normalize.NormalizeDriver;
import org.ankus.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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


/*
 * KMeansDriver
 * @desc map/reduce based k-means clustering
 *
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class KMeansDriver extends Configured implements Tool {

    private Logger logger = LoggerFactory.getLogger(KMeansDriver.class);

	private String mNominalDelimiter = "@@";     // delimiter for nominal attribute in cluster info
	private int mIndexArr[];                      // attribute index array will be used clustering
	private int mNominalIndexArr[];              // nominal attribute index array will be used clustering
	private int mExceptionIndexArr[];           // attribute index array will be not used clustering
	
	@Override
	public int run(String[] args) throws Exception {

        logger.info("K-Means Clustering MR-Job is Started..");

		Configuration conf = new Configuration();
		if(!ConfigurationVariable.setFromArguments(args, conf))
		{
			System.err.println("MR Job Setting Failed..");
            Usage.printUsage(Constants.ALGORITHM_KMEANS_CLUSTERING);
            logger.info("K-Means Clustering MR-Job is Failed..: Configuration Failed");
            return 1;
		}

		if(conf.get(ArgumentsConstants.NORMALIZE, "true").equals("true"))
		{
            logger.info("Normalization for K-Means is Started..");
			String normalizationJobOutput = conf.get(ArgumentsConstants.OUTPUT_PATH, null) + "/normalize";
			String params[] = getParametersForNormalization(conf, normalizationJobOutput);
			
			NormalizeDriver stats = new NormalizeDriver();
			if(stats.run(params)!=0)
            {
                logger.info("Normalization for K-Means is Failed..");
                return 1;
            }

            logger.info("Normalization for K-Means is Successfully Finished..");
			conf.set(ArgumentsConstants.INPUT_PATH, normalizationJobOutput);
		}
		
		/**
		 * clustering process
		 * 1. set initial cluster center (old-cluster): Main Driver Class
		 * 		numeric - distribution based ( min + ((max-min)/clusterCnt*clusterIndex)) 
		 * 		nominal - frequency ratio
		 * 2.assign cluster to each data and update cluster center (MR Job) 
		 * 		2.1 assign cluster to data using cluster-data distance (for all data): Map
		 * 		2.2 update cluster center (new-cluster): Reduce
		 * 4. compare old-cluster / new-cluster: Main Driver Class
		 * 5. decision: Main Driver Class
		 * 		if(euqal) exec 2.1 and finish
		 * 		else if(maxIter) exec 2.1 and finish
		 * 		else 
		 * 		{
		 * 			pre-cluster <= post-cluster
		 * 			goto Step 2
		 * 		}
		 * 
		 * cluster representation
		 * 0. id
		 * 1. numeric => index '' value
		 * 2. nominal => index '' value@@ratio[@@value@@ratio]+
		 * 
		 * cluster-data distance
		 * 1. numeric => clusterVal - dataVal
		 * 2. nominal => 1 - ratio(cluster=data)
		 * => Total Distance (Euclidean / Manhatan)
		 */
        logger.info("Core Part for K-Means is Started..");

		setIndexArray(conf);
		
		int iterCnt = 0;
		String outputBase = conf.get(ArgumentsConstants.OUTPUT_PATH, null);


		String oldClusterPath = outputBase + "/cluster_center_0";
		String newClusterPath;

        logger.info("> Cluster Center Initializing is Started....");
        setInitialClusterCenter(conf, oldClusterPath);							// init cluster
        logger.info("> Cluster Center Initializing is Finished....");

        logger.info("> Iteration(Cluster Assign / Cluster Update) is Started....");
		while(true)
		{
			iterCnt++;
            logger.info("> Iteration: " + iterCnt);

			newClusterPath = outputBase + "/cluster_center_" + iterCnt;
			conf.set(ArgumentsConstants.OUTPUT_PATH, newClusterPath);
            logger.info(">> MR-Job for Cluster Assign / Cluster Update is Started..");
            if(!assignAndResetCluster(conf, oldClusterPath))
            {
                logger.info(">> MR-Job for Cluster Assign / Cluster Update is Failed..");
                return 1;			// MR Job, assign and update cluster
            }
            logger.info(">> MR-Job for Cluster Assign / Cluster Update is Finished...");

            logger.info(">> Iteration Break Condition Check..");
			if(isClustersEqual(conf, oldClusterPath, newClusterPath)) break;	// cluster check
			else if(iterCnt >= Integer.parseInt(conf.get(ArgumentsConstants.MAX_ITERATION, "1"))) break;

            logger.info(">> Iteration is not Broken. Continue Next Iteration..");
			oldClusterPath = newClusterPath;
		}
        logger.info(">> Iteration is Broken..");
        logger.info("> Iteration(Cluster Assign / Cluster Update) is Finished....");
		
		conf.set(ArgumentsConstants.OUTPUT_PATH, outputBase + "/clustering_result");
		if(!finalAssignCluster(conf, newClusterPath)) return 1;					// Map Job, final assign cluster
        logger.info("> Final Cluster Assign and Compute Distance..");


		if(conf.get(ArgumentsConstants.TEMP_DELETE, "true").equals("true"))
        {
            logger.info("Temporary Files are Deleted..: Cluster Center Info Files");
			for(int i=0; i<iterCnt; i++)
			{
				FileSystem.get(conf).delete(new Path(outputBase + "/cluster_center_" + i), true);
			}
        }

        logger.info("Core Part for K-Means is Successfully Finished...");
		return 0;
	}

    /*
    * @desc initialize cluster center info (file generation)
    *
    * @parameter
    *       conf        configuration identifier for job (non-mr)
    *       clusterOutputPath       file path for cluster info
    * @return
    */
	private void setInitialClusterCenter(Configuration conf, String clusterOutputPath) throws Exception
	{
		/**
		 * TODO:
		 * Current Process
		 * 		- get top n data (n is defined cluster count)
		 * 		- set each data to initial cluster center
		 * 
		 * Following Process is reasonable. => MR Job
		 * 		1. Distribution
		 * 			- get statistics(distribution) for all attributes
		 * 			- use min/max and freq for initial cluster center setting
		 * 		numeric => (max-min) / cluster count
		 * 		nominal => each value (freq sort) 
		 */
		FileSystem fs = FileSystem.get(conf);
				
		String readStr, tokens[];
		int index = 0;
		int clusterCnt = Integer.parseInt(conf.get(ArgumentsConstants.CLUSTER_COUNT, "1"));
		KMeansClusterInfoMgr clusters[] = new KMeansClusterInfoMgr[clusterCnt];
		

		Path inputPath = new Path(conf.get(ArgumentsConstants.INPUT_PATH, null));
		if(!fs.isFile(inputPath))
		{
			boolean isFile = false;
			while(!isFile)
			{
				FileStatus[] status = fs.listStatus(inputPath);
		        if(fs.isFile(status[0].getPath())) isFile = true;
		        
		        inputPath = status[0].getPath();
			}
		}
		
		FSDataInputStream fin = fs.open(inputPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
		
		while((readStr = br.readLine())!=null)
		{
			clusters[index] = new KMeansClusterInfoMgr();
			clusters[index].setClusterID(index);
			
			tokens = readStr.split(conf.get(ArgumentsConstants.DELIMITER, "\t"));
			for(int i=0; i<tokens.length; i++)
			{
				if(CommonMethods.isContainIndex(mIndexArr, i, true)
						&& !CommonMethods.isContainIndex(mExceptionIndexArr, i, false))
				{
					if(CommonMethods.isContainIndex(mNominalIndexArr, i, false))
					{
						clusters[index].addAttributeValue(i, tokens[i], ConfigurationVariable.NOMINAL_ATTRIBUTE);
					}
					else clusters[index].addAttributeValue(i, tokens[i], ConfigurationVariable.NUMERIC_ATTRIBUTE);
				}
			}
			
			index++;
			if(index >= clusterCnt) break;
		}
		
		br.close();
		fin.close();
		
		FSDataOutputStream fout = fs.create(new Path(clusterOutputPath + "/part-r-00000"), true);		
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));

		for(int i=0; i<clusters.length; i++)
		{
			bw.write(clusters[i].getClusterInfoString(conf.get(ArgumentsConstants.DELIMITER, "\t"), mNominalDelimiter) + "\n");
		}
		
		bw.close();
		fout.close();
		
	}

    /*
    * @desc kmenas update mr job - cluster assign and cluster info update
    *
    * @parameter
    *       conf        configuration identifier for job
    *       oldClusterPath       file path for ole cluster info (before kmeans update)
    * @return
    *       boolean - if mr job is successfully finished
    */
	private boolean assignAndResetCluster(Configuration conf, String oldClusterPath) throws Exception
	{
		/**
		 * Map Job
		 * 		- load old cluster center
		 * 		- each data -> compute distance to each cluster
		 * 		- assign cluster number using distance: Map Key for Reduce
		 * 
		 * Reduce Job
		 * 		- compute new center
		 * 		- save key and computed center info
		 */
		
		Job job = new Job();  
		
		FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
		
		job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
		job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
		job.getConfiguration().set(ArgumentsConstants.NOMINAL_INDEX, conf.get(ArgumentsConstants.NOMINAL_INDEX, "-1"));
		job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
		job.getConfiguration().set(ArgumentsConstants.CLUSTER_COUNT, conf.get(ArgumentsConstants.CLUSTER_COUNT, "1"));		
		job.getConfiguration().set(ArgumentsConstants.CLUSTER_PATH, oldClusterPath);
		job.getConfiguration().set("subDelimiter", mNominalDelimiter);
		
		
		job.setJarByClass(KMeansDriver.class);
		
		// Mapper & Reducer Class
		job.setMapperClass(KMeansClusterAssignMapper.class);
		job.setReducerClass(KMeansClusterUpdateReducer.class);

        // Mapper Output Key & Value Type after Hadoop 0.20
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

        // Reducer Output Key & Value Type
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
//		job.setNumReduceTasks(0);
		
		if(!job.waitForCompletion(true))
    	{
        	System.err.println("Error: MR for KMeans(Rutine) is not Completeion");
        	return false;
        }
		
        return true;
	}

    /*
    * @desc compare old and new cluster are equal
    *
    * @parameter
    *       conf        configuration identifier for job (non-mr)
    *       oldClusterPath       file path for ole cluster info (before kmeans update)
    * @return
    *       boolean - if job is successfully finished
    */
	private boolean isClustersEqual(Configuration conf, String oldClusterPath, String newClusterPath) throws Exception
	{
		/**
		 * Check clusters are equal (cluster index and center info)
		 * Load 2 files
		 * 		HashMap Structure: Key - Cluster Index
		 * 							Value - Cluster Center Info. String
		 * for each Value of Keys, check
		 */
		
		int clusterCnt = Integer.parseInt(conf.get(ArgumentsConstants.CLUSTER_COUNT, "1"));
		String delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
				
		KMeansClusterInfoMgr oldClusters[] = KMeansClusterInfoMgr.loadClusterInfoFile(conf, new Path(oldClusterPath), clusterCnt, delimiter);
		KMeansClusterInfoMgr newClusters[] = KMeansClusterInfoMgr.loadClusterInfoFile(conf, new Path(newClusterPath), clusterCnt, delimiter);
		
		for(int i=0; i<clusterCnt; i++)
		{	
			if(!oldClusters[i].isEqualClusterInfo(newClusters[i])) return false;
		}
		
		return true;
	}

    /*
    * @desc final cluster assign to each data
    *
    * @parameter
    *       conf        configuration identifier for job
    *       clusterPath       file path for final cluster info
    * @return
    *       boolean - if mr job is successfully finished
    */
	private boolean finalAssignCluster(Configuration conf, String clusterPath) throws Exception
	{
		/**
		 * Map Job (ref. Map job of 'assignAndResetCluster()')
		 * 
		 * If cat use MR default delimiter then, use Map job of 'assignAndResetCluster()'
		 * else, * Modified Map Job for Writing (no key, key is used to last attribute)
		 */
		Job job = new Job();
		
		FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
		
		job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
		job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
		job.getConfiguration().set(ArgumentsConstants.NOMINAL_INDEX, conf.get(ArgumentsConstants.NOMINAL_INDEX, "-1"));
		job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
		job.getConfiguration().set(ArgumentsConstants.CLUSTER_COUNT, conf.get(ArgumentsConstants.CLUSTER_COUNT, "1"));		
		job.getConfiguration().set(ArgumentsConstants.CLUSTER_PATH, clusterPath);
		job.getConfiguration().set("nominalDelimiter", mNominalDelimiter);
		
		
		job.setJarByClass(KMeansDriver.class);
		
		// Mapper & Reducer Class
		job.setMapperClass(KMeansClusterAssignFinalMapper.class);

        // Mapper Output Key & Value Type after Hadoop 0.20
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

        // Reducer Output Key & Value Type
		job.setNumReduceTasks(0);
		
		if(!job.waitForCompletion(true))
    	{
        	System.err.println("Error: MR for KMeans(Final) is not Completion");
        	return false;
        }
		
        return true;
	}




    /*
    * @desc arguments setting for normalization mr job
    *
    * @parameter
    *       conf        configuration identifier for job
    *       outputPath       output path for normalization job
    * @return
    *       string array - arguments for normalization job
    */
	private String[] getParametersForNormalization(Configuration conf, String outputPath) throws Exception 
	{
		String params[] = new String[16];
		
		params[0] = ArgumentsConstants.INPUT_PATH;
		params[1] = conf.get(ArgumentsConstants.INPUT_PATH, null);
		
		params[2] = ArgumentsConstants.OUTPUT_PATH;
		params[3] = outputPath;
		
		params[4] = ArgumentsConstants.DELIMITER;
		params[5] = conf.get(ArgumentsConstants.DELIMITER, "\t");
		
		params[6] = ArgumentsConstants.TARGET_INDEX;
		params[7] = conf.get(ArgumentsConstants.TARGET_INDEX, "-1");
		
		String nominalIndexList = conf.get(ArgumentsConstants.NOMINAL_INDEX, "-1");
		String exceptionIndexList = conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1");
		if(!nominalIndexList.equals("-1"))
		{
			if(exceptionIndexList.equals("-1")) exceptionIndexList = nominalIndexList;
			else exceptionIndexList += "," + nominalIndexList;
		}
		params[8] = ArgumentsConstants.EXCEPTION_INDEX;
		params[9] = exceptionIndexList;
		
		params[10] = ArgumentsConstants.REMAIN_FIELDS;
		params[11] = "true";
		
		params[12] = ArgumentsConstants.TEMP_DELETE;
		params[13] = conf.get(ArgumentsConstants.TEMP_DELETE, "true");
		
		params[14] = ArgumentsConstants.MR_JOB_STEP;
		params[15] = conf.get(ArgumentsConstants.MR_JOB_STEP, "1");
		
		return params;
	}

    /*
    * @desc convert from comma based string index list to int array
    *
    * @parameter
    *       conf        configuration identifier for job
    * @return
    */
	private void setIndexArray(Configuration conf)
	{
		mIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX,  "-1"));
		mNominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1"));
		mExceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));
	}
	
	
	
	public static void main(String args[]) throws Exception 
	{
		int res = ToolRunner.run(new KMeansDriver(), args);
        System.exit(res);
	}
	

}
