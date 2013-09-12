package org.ankus.mapreduce.algorithms.clustering.em;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.ankus.mapreduce.algorithms.preprocessing.normalize.NormalizeDriver;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.ankus.util.ConfigurationVariable;
import org.ankus.util.Constants;
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

public class EM extends Configured implements Tool {
	
	private String m_subDelimiter = "@@";
	private int m_indexArr[];
	private int m_nominalIndexArr[];
	private int m_exceptionIndexArr[];
	
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		if(!ConfigurationVariable.setFromArguments(args, conf))
		{
			System.err.println("MR Job Setting Failed..");
			printUsage();
			return 1;		
		}
		
		if(conf.get(ArgumentsConstants.HELP, "false").equals("true"))
		{
			printUsage();
			return  0;
		}
		
		
		if(conf.get(ArgumentsConstants.NORMALIZE, "false").equals("true"))
		{
			String nomalizationJobOutput = conf.get(ArgumentsConstants.OUTPUT_PATH, null) + "/normalize";
			String params[] = getParametersForNormalization(conf, nomalizationJobOutput);
			
			NormalizeDriver stats = new NormalizeDriver();
			if(stats.run(params)!=0) return 1;
			
			conf.set(ArgumentsConstants.INPUT_PATH, nomalizationJobOutput);
			
		}
		
		/**
		 * TODO:
		 * clustering process
		 * 1. set initial cluster center (old-cluster): Main Driver Class
		 * 		numeric - statistics based pdf function (avg=value, variance=avg/5 or 10) 
		 * 		nominal - frequency ratio
		 * 2. assign cluster to each data and update cluster center (MR Job) 
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
		 * 
		 * cluster representation
		 * 0. id
		 * 1. numeric => index '' average@@variance
		 * 2. nominal => index '' value@@ratio[@@value@@ratio]+
		 * 
		 * cluster-data distance
		 * 1. numeric => 1 - pdf function value
		 * 2. nominal => 1 - ratio(cluster=data)
		 * => Total Distance (Average / CFSum / GeoAvg / HarAvg)
		 */
		setIndexArray(conf);
		
		int iterCnt = 0;
		String outputBase = conf.get(ArgumentsConstants.OUTPUT_PATH, null);
		
		String oldClusterPath = outputBase + "/cluster_center_0";
		String newClusterPath;
		setInitialClusterCenter(conf, oldClusterPath);							// init cluster
		while(true)
		{
			iterCnt++;			
			newClusterPath = outputBase + "/cluster_center_" + iterCnt;
			conf.set(ArgumentsConstants.OUTPUT_PATH, newClusterPath);
			if(!assignAndResetCluster(conf, oldClusterPath)) return 1;			// MR Job, assign and update cluster
			
			if(isClustersEqual(conf, oldClusterPath, newClusterPath)) break;	// cluster check
			else if(iterCnt >= Integer.parseInt(conf.get(ArgumentsConstants.MAX_ITERATION, "1"))) break;
			
			oldClusterPath = newClusterPath;
		}
		
		conf.set(ArgumentsConstants.OUTPUT_PATH, outputBase + "/clustering_result");
		if(!finalAssignCluster(conf, newClusterPath)) return 1;					// Map Job, final assign cluster
		
		
		if(conf.get(ArgumentsConstants.TEMP_DELETE, "true").equals("true"))
        {
			for(int i=0; i<iterCnt; i++)
			{
				FileSystem.get(conf).delete(new Path(outputBase + "/cluster_center_" + i), true);
			}
        }
		
		return 0;
	}
	

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
		EMClusterStructure clusters[] = new EMClusterStructure[clusterCnt];		

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
			clusters[index] = new EMClusterStructure();
			clusters[index].setClusterID(index);
			
			for(int k=0; k<2; k++)
			{	
				tokens = readStr.split(conf.get(ArgumentsConstants.DELIMITER, "\t"));
				for(int i=0; i<tokens.length; i++)
				{
					if(CommonMethods.isContainIndex(m_indexArr, i, true)
							&& !CommonMethods.isContainIndex(m_exceptionIndexArr, i, false))
					{
						if(CommonMethods.isContainIndex(m_nominalIndexArr, i, false))
						{
							clusters[index].addAttributeValue(i, tokens[i], ConfigurationVariable.NOMINAL_ATTRIBUTE);
						}
						else clusters[index].addAttributeValue(i, tokens[i], ConfigurationVariable.NUMERIC_ATTRIBUTE);
					}
				}
				
				if(k==0) readStr = br.readLine();
			}
			clusters[index].finalCompute(2);
			
			index++;
			if(index >= clusterCnt) break;
		}
		
		br.close();
		fin.close();
		
		FSDataOutputStream fout = fs.create(new Path(clusterOutputPath + "/part-r-00000"), true);		
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));
		
		for(int i=0; i<clusters.length; i++)
		{
			bw.write(clusters[i].getClusterInfoString(conf.get(ArgumentsConstants.DELIMITER, "\t"), m_subDelimiter) + "\n");
		}
		
		bw.close();
		fout.close();
		
	}
	
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
		job.getConfiguration().set("subDelimiter", m_subDelimiter);
		
		
		job.setJarByClass(EM.class);
		
		// Mapper & Reducer Class
		job.setMapperClass(EMMapper.class);
		job.setReducerClass(EMReducer.class);

        // Mapper Output Key & Value Type after Hadoop 0.20
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

        // Reducer Output Key & Value Type
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
//		job.setNumReduceTasks(0);
		
		if(!job.waitForCompletion(true))
    	{
        	System.err.println("Error: MR for EM(Rutine) is not Completeion");
        	return false;
        }
		
        return true;
	}
	
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

		EMClusterStructure oldClusters[] = EMClusterStructure.loadClusterInfoFile(conf, new Path(oldClusterPath), clusterCnt, delimiter);
		EMClusterStructure newClusters[] = EMClusterStructure.loadClusterInfoFile(conf, new Path(newClusterPath), clusterCnt, delimiter);

		for(int i=0; i<clusterCnt; i++)
		{
			if(!oldClusters[i].isEqualClusterInfo(newClusters[i])) return false;
		}

		return true;
	}
	
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
		job.getConfiguration().set("nominalDelimiter", m_subDelimiter);
		
		
		job.setJarByClass(EM.class);
		
		// Mapper & Reducer Class
		job.setMapperClass(EMMapper_Final.class);

        // Mapper Output Key & Value Type after Hadoop 0.20
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

        // Reducer Output Key & Value Type
		job.setNumReduceTasks(0);
		
		if(!job.waitForCompletion(true))
    	{
        	System.err.println("Error: MR for EM(Final) is not Completeion");
        	return false;
        }
		
        return true;
	}
	
	
	
	
	
	
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
	
	private void setIndexArray(Configuration conf)
	{
		m_indexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX,  "-1"));
		m_nominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1"));
		m_exceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));
	}
	
	public static void main(String args[]) throws Exception 
	{
		int res = ToolRunner.run(new EM(), args);
        System.exit(res);
	}
	
	private void printUsage()
	{
		
	}

}
