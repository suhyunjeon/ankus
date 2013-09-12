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
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.ankus.util.ConfigurationVariable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * KMeansClusterInfoMgr
 * @desc class for cluster info structure and management
 *          used in KMeansDriver
 *
 * @version 0.0.1
 * @date : 2013.08.22
 * @author Moonie
 */
public class KMeansClusterInfoMgr {
	

	public int mClusterId = -1;           // cluster identifier
	
	private HashMap<Integer, Double> mNumericValueList;                     // numeric features info of cluster
	private HashMap<Integer, HashMap<String, Double>> mNominalValueList;    // nominal features info of cluster

    /*
    * @desc initialize class object
    *
    * @parameter
    * @return
    */
	public KMeansClusterInfoMgr()
	{
		mNumericValueList = new HashMap<Integer, Double>();
		mNominalValueList = new HashMap<Integer, HashMap<String, Double>>();
	}

    /*
    * @desc set cluster identifier
    *
    * @parameter
    *       id      integer identifier for cluster
    * @return
    */
	public void setClusterID(int id)
	{
		mClusterId = id;
	}

    /*
    * @desc add new data info to cluster
    *
    * @parameter
    *       index       target cluster index
    *       value       value to update
    *       dataType    data type - numeric or nominal
    * @return
    *       boolean     if non-numeric/nominal data, then return false
    */
	public boolean addAttributeValue(int index, String value, String dataType)
	{
		if(dataType.equals(ConfigurationVariable.NUMERIC_ATTRIBUTE))
		{
			double val = Double.parseDouble(value);
			if(mNumericValueList.containsKey(index)) val += mNumericValueList.get(index);
			mNumericValueList.put(index, val);
		}
		else if(dataType.equals(ConfigurationVariable.NOMINAL_ATTRIBUTE))
		{
			if(mNominalValueList.containsKey(index))
			{
				HashMap<String, Double> attrList = mNominalValueList.get(index);
				if(attrList.containsKey(value)) attrList.put(value, attrList.get(value) + 1.0);
				else
				{
					attrList.put(value, 1.0);
					mNominalValueList.put(index, attrList);
				}
			}
			else 
			{
				HashMap<String, Double> newAttr = new HashMap<String, Double>();
				newAttr.put(value, 1.0);
				mNominalValueList.put(index, newAttr);
			}
		}
		else return false;
		
		return true;
	}

    /*
    * @desc update cluster center information using appropriate datas
    *
    * @parameter
    *       dataCnt       data count assigned this cluster
    * @return
    */
	public void finalCompute(int dataCnt)
	{
		Iterator<Integer> numericKeySetIter = mNumericValueList.keySet().iterator();
		while(numericKeySetIter.hasNext())
		{
			int key = numericKeySetIter.next();
			mNumericValueList.put(key, mNumericValueList.get(key) / (double)dataCnt);
		}
		
		Iterator<Integer> nominalKeySetIter = mNominalValueList.keySet().iterator();
		while(nominalKeySetIter.hasNext())
		{
			int key = nominalKeySetIter.next();			
			HashMap<String, Double> valueMap = mNominalValueList.get(key);
			
			Iterator<String> valueKeyIter = valueMap.keySet().iterator();
			while(valueKeyIter.hasNext())
			{
				String valueKey = valueKeyIter.next();
				valueMap.put(valueKey, valueMap.get(valueKey) / (double)dataCnt);
			}
			
			mNominalValueList.put(key, valueMap);
		}
	}

    /*
    * @desc generate string value for file writing of this cluster info
    *
    * @parameter
    *       delimiter       delimiter for attribute separation
    *       subDelimiter    delimiter for features in nominal attribute
    * @return
    *       string value of cluster info
    */
	public String getClusterInfoString(String delimiter, String subDelimiter)
	{
		// id setting
		String retStr = mClusterId + "";
		
		// numeric value setting
		Iterator<Integer> numericKeySetIter = mNumericValueList.keySet().iterator();
		while(numericKeySetIter.hasNext())
		{
			int key = numericKeySetIter.next();
			retStr += delimiter + key + delimiter + mNumericValueList.get(key);
		}		
		
		// nominal value setting 
		Iterator<Integer> nominalKeySetIter = mNominalValueList.keySet().iterator();
		while(nominalKeySetIter.hasNext())
		{
			int key = nominalKeySetIter.next();			
			HashMap<String, Double> valueMap = mNominalValueList.get(key);
			
			Iterator<String> valueKeyIter = valueMap.keySet().iterator();
			String valueValueStr = "";
			while(valueKeyIter.hasNext())
			{
				String valueKey = valueKeyIter.next();
				valueValueStr += subDelimiter + valueKey + subDelimiter + valueMap.get(valueKey);
			}
			
			retStr += delimiter + key + delimiter + valueValueStr.substring(subDelimiter.length());
		}
		
		return retStr;
	}

    /*
    * @desc load cluster info from cluster info string
    *
    * @parameter
    *       inputStr        string types cluster info
    *       delimiter       delimiter for attribute separation
    *       subDelimiter    delimiter for features in nominal attribute
    * @return
    */
	public void loadClusterInfoString(String inputStr, String delimiter, String subDelimiter)
	{
		mNumericValueList = new HashMap<Integer, Double>();
		mNominalValueList = new HashMap<Integer, HashMap<String,Double>>();
		
		String tokens[] = inputStr.split(delimiter);
		
		mClusterId = Integer.parseInt(tokens[0]);
		
		for(int i=1; i<tokens.length; i++)
		{
			int attrIndex = Integer.parseInt(tokens[i++]);
			String attrValue = tokens[i];
			
			if(!attrValue.contains(subDelimiter)) mNumericValueList.put(attrIndex, Double.parseDouble(attrValue));
			else
			{
				// nominal
				String subTokens[] = attrValue.split(subDelimiter);
				HashMap<String, Double> valueMap = new HashMap<String, Double>();
				for(int k=0; k<subTokens.length; k++)
				{
					String subKey = subTokens[k++];
					double subValue = Double.parseDouble(subTokens[k]);
					valueMap.put(subKey, subValue);
				}
				mNominalValueList.put(attrIndex, valueMap);
			}
		}
	}

    /*
    * @desc load all cluster info from cluster info file
    *
    * @parameter
    *       conf            configuration identifier
    *       clusterPath     file path for loading
    *       clusterCnt      cluster count to load
    *       delimiter       delimiter for attribute separation
    * @return
    *       cluster info array
    */
	public static KMeansClusterInfoMgr[] loadClusterInfoFile(Configuration conf, Path clusterPath, int clusterCnt, String delimiter) throws IOException
	{
		KMeansClusterInfoMgr[] clusters = new KMeansClusterInfoMgr[clusterCnt];
		
		FileStatus[] status = FileSystem.get(conf).listStatus(clusterPath);		
		for(int i=0; i<status.length; i++)
		{
			FSDataInputStream fin = FileSystem.get(conf).open(status[i].getPath());
			BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
			String readStr;
			while((readStr = br.readLine())!=null)
			{	
				int clusterId = Integer.parseInt(readStr.substring(0, readStr.indexOf(delimiter)));
				clusters[clusterId] = new KMeansClusterInfoMgr();
				clusters[clusterId].loadClusterInfoString(readStr, delimiter, conf.get("subDelimiter", "@@"));
			}
			br.close();
			fin.close();
		}
		
		return clusters;
	}

    /*
    * @desc get distance of specified attribute between received value and this cluster
    *
    * @parameter
    *       attrIndex     attribute index for compare
    *       attrValue     attribute value for compare
    *       attrType      attribute type for compare (numeric/nominal)
    * @return
    *       double type distance value
    */
	public double getAttributeDistance(int attrIndex, String attrValue, String attrType)
	{
		if(attrType.equals(ConfigurationVariable.NUMERIC_ATTRIBUTE))
		{
			if(mNumericValueList.containsKey(attrIndex)) return mNumericValueList.get(attrIndex) - Double.parseDouble(attrValue);
			else return Double.parseDouble(attrValue);
		}
		else if(attrType.equals(ConfigurationVariable.NOMINAL_ATTRIBUTE))
		{
			if(mNominalValueList.containsKey(attrIndex))
			{
				HashMap<String, Double> valueMap = mNominalValueList.get(attrIndex);
				if(valueMap.containsKey(attrValue))
				{
					return 1 - valueMap.get(attrValue);
				}
				else return 1;
			}
			else return 1;
		}
		
		return 1;
	}

    /*
    * @desc check received cluster's center and this cluster's center are equal
    *
    * @parameter
    *       cluster     cluster for compare
    * @return
    *       boolean - if centers of clusters are equal then return true
    */
	public boolean isEqualClusterInfo(KMeansClusterInfoMgr cluster)
	{
		Iterator<Integer> numericAttrIndexIter = mNumericValueList.keySet().iterator();
		while(numericAttrIndexIter.hasNext())
		{
			int attrIndex = numericAttrIndexIter.next();
			
			if(!cluster.mNumericValueList.containsKey(attrIndex)) return false;
			else if(!mNumericValueList.get(attrIndex).equals(cluster.mNumericValueList.get(attrIndex))) return false;
		}
		
		
		Iterator<Integer> nominalAttrIndexIter = mNominalValueList.keySet().iterator();
		while(nominalAttrIndexIter.hasNext())
		{	
			int attrIndex = nominalAttrIndexIter.next();
			
			if(!cluster.mNominalValueList.containsKey(attrIndex)) return false;
			else
			{
				HashMap<String, Double> valueMap = mNominalValueList.get(attrIndex);
				HashMap<String, Double> clusterValueMap = cluster.mNominalValueList.get(attrIndex);
				
				Iterator<String> valueNameIter = valueMap.keySet().iterator();
				while(valueNameIter.hasNext())
				{
					String name = valueNameIter.next();
					
					if(!clusterValueMap.containsKey(name)) return false;
					else if(!valueMap.get(name).equals(clusterValueMap.get(name))) return false;
				}
			}
		}
		
		return true;
	}
	
	
	
	
	

}
