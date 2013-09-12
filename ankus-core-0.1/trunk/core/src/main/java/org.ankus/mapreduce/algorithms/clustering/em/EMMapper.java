package org.ankus.mapreduce.algorithms.clustering.em;

import java.io.IOException;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.ankus.util.ConfigurationVariable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EMMapper extends Mapper<Object, Text, IntWritable, Text>{

	String m_delimiter;

	int m_indexArr[];
	int m_nominalIndexArr[];
	int m_exceptionIndexArr[];

	int m_clusterCnt;
	EMClusterStructure clusters[];


	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{

	}

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		String[] columns = value.toString().split(m_delimiter);
		int clusterIndex = -1;

		/**
		 * cluster index get
		 */
		double probMax = 0;
		for(int k=0; k<m_clusterCnt; k++)
		{
			double attrProbSum = 0;
			double attrCnt = 0;

			/**
			 * TODO: total distance - euclidean
			 */
			for(int i=0; i<columns.length; i++)
			{
				double probAttr = 0;

				if(CommonMethods.isContainIndex(m_indexArr, i, true)
						&& !CommonMethods.isContainIndex(m_exceptionIndexArr, i, false))
				{
					attrCnt = attrCnt + 1;
					if(CommonMethods.isContainIndex(m_nominalIndexArr, i, false))
					{
						probAttr = clusters[k].getAttributeProbability(i, columns[i], ConfigurationVariable.NOMINAL_ATTRIBUTE);
					}
					else probAttr = clusters[k].getAttributeProbability(i, columns[i], ConfigurationVariable.NUMERIC_ATTRIBUTE);

					attrProbSum += probAttr;
				}

			}

			double prob = attrProbSum / attrCnt;
			if(prob >= probMax)
			{
				probMax = prob;
				clusterIndex = k;
			}
		}

		context.write(new IntWritable(clusterIndex), value);
	}


	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();

		m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");

		m_indexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX,  "-1"));
		m_nominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1"));
		m_exceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));

		m_clusterCnt = Integer.parseInt(conf.get(ArgumentsConstants.CLUSTER_COUNT, "1"));


		// cluster load and setting
		Path clusterPath = new Path(conf.get(ArgumentsConstants.CLUSTER_PATH, null));
		clusters = EMClusterStructure.loadClusterInfoFile(conf, clusterPath, m_clusterCnt, m_delimiter);
	}

	

}
