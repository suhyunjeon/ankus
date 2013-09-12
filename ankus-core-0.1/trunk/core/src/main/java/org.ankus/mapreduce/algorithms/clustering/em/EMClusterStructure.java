package org.ankus.mapreduce.algorithms.clustering.em;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.ankus.util.CommonMethods;
import org.ankus.util.ConfigurationVariable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class EMClusterStructure {
	
	private double baseVarianceParameter = 10; 
	
	private class PDF_Features {
		public double sum;
		public double squareSum;
		
		public double average;
		public double variance;
		
		public PDF_Features()
		{
			sum = 0;
			squareSum = 0;
			average = 0;
			variance = 0;
		}
		
		public boolean equals(PDF_Features value)
		{
			if((this.average==value.average) && (this.variance==value.variance)) return true;
			else return false;
		}
	}
	
	public int m_clusterId = -1;
	
	private HashMap<Integer, PDF_Features> m_numericValueList;
	private HashMap<Integer, HashMap<String, Double>> m_nominalValueList;
	
	public EMClusterStructure()
	{
		m_numericValueList = new HashMap<Integer, PDF_Features>();
		m_nominalValueList = new HashMap<Integer, HashMap<String, Double>>();
	}
	
	public void setClusterID(int id)
	{
		m_clusterId = id;
	}
	
	// for all attribute..
	public boolean addAttributeValue(int index, String value, String dataType)
	{
		if(dataType.equals(ConfigurationVariable.NUMERIC_ATTRIBUTE))
		{	
			PDF_Features valueClass = new PDF_Features();
			if(m_numericValueList.containsKey(index)) valueClass = m_numericValueList.get(index);
		
			double val = Double.parseDouble(value);
			valueClass.sum += val;
			valueClass.squareSum += Math.pow(val, 2);
			
			m_numericValueList.put(index, valueClass);
		}
		else if(dataType.equals(ConfigurationVariable.NOMINAL_ATTRIBUTE))
		{
			if(m_nominalValueList.containsKey(index))
			{
				HashMap<String, Double> attrList = m_nominalValueList.get(index);
				if(attrList.containsKey(value)) attrList.put(value, attrList.get(value) + 1.0);
				else
				{
					attrList.put(value, 1.0);
					m_nominalValueList.put(index, attrList);
				}
			}
			else 
			{
				HashMap<String, Double> newAttr = new HashMap<String, Double>();
				newAttr.put(value, 1.0);
				m_nominalValueList.put(index, newAttr);						
			}
		}
		else return false;
		
		return true;
	}
	
	// for cluster center update reduce job
	public void finalCompute(int dataCnt)
	{
		Iterator<Integer> numericKeySetIter = m_numericValueList.keySet().iterator();
		while(numericKeySetIter.hasNext())
		{
			int key = numericKeySetIter.next();
			
			PDF_Features value = m_numericValueList.get(key);
			value.average = value.sum / (double)dataCnt;
			value.variance = (value.squareSum/(double)dataCnt) - Math.pow(value.average,2);
			
//			if(value.variance == 0) value.variance = value.average / baseVarianceParameter;
			if(value.variance == 0) value.variance = 1;
			
			m_numericValueList.put(key, value);

		}
		
		Iterator<Integer> nominalKeySetIter = m_nominalValueList.keySet().iterator();
		while(nominalKeySetIter.hasNext())
		{
			int key = nominalKeySetIter.next();			
			HashMap<String, Double> valueMap = m_nominalValueList.get(key);
			
			Iterator<String> valueKeyIter = valueMap.keySet().iterator();
			while(valueKeyIter.hasNext())
			{
				String valueKey = valueKeyIter.next();
				valueMap.put(valueKey, valueMap.get(valueKey) / (double)dataCnt);
			}
			
			m_nominalValueList.put(key, valueMap);
		}
	}
	

	public double getAttributeProbability(int attrIndex, String attrValue, String attrType)
	{
		if(attrType.equals(ConfigurationVariable.NUMERIC_ATTRIBUTE))
		{
			if(m_numericValueList.containsKey(attrIndex))
			{
				PDF_Features pdfValue = m_numericValueList.get(attrIndex);
				if(pdfValue.variance==0)
				{
					if(Double.parseDouble(attrValue) == pdfValue.average) return 1;
					else return 0;
				}
				else
				{	
					double powVal = (-1 * Math.pow((Double.parseDouble(attrValue)-pdfValue.average), 2)) / (2 * pdfValue.variance);
					double baseVal = 1 / Math.sqrt(2 * Math.PI * pdfValue.variance);		
					
					return baseVal * Math.pow(Math.E, powVal);
				}
			}
			else return 0;
		}
		else if(attrType.equals(ConfigurationVariable.NOMINAL_ATTRIBUTE))
		{
			if(m_nominalValueList.containsKey(attrIndex))
			{
				HashMap<String, Double> valueMap = m_nominalValueList.get(attrIndex);
				if(valueMap.containsKey(attrValue))
				{
					return valueMap.get(attrValue);
				}
				else return 0;
			}
			else return 0;
		}
		
		return 0;
	}
	
	
	// get write string
	public String getClusterInfoString(String delimiter, String subDelimiter)
	{
		// id setting
		String retStr = m_clusterId + "";
		
		// numeric value setting
		Iterator<Integer> numericKeySetIter = m_numericValueList.keySet().iterator();
		while(numericKeySetIter.hasNext())
		{
			int key = numericKeySetIter.next();
			PDF_Features value = m_numericValueList.get(key);
			retStr += delimiter + key + delimiter + value.average + subDelimiter + value.variance;
		}		
		
		// nominal value setting 
		Iterator<Integer> nominalKeySetIter = m_nominalValueList.keySet().iterator();
		while(nominalKeySetIter.hasNext())
		{
			int key = nominalKeySetIter.next();			
			HashMap<String, Double> valueMap = m_nominalValueList.get(key);
			
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
	
	// load from written string
	public void loadClusterInfoString(String inputStr, String delimiter, String subDelimiter)
	{
		m_numericValueList = new HashMap<Integer, PDF_Features>();
		m_nominalValueList = new HashMap<Integer, HashMap<String,Double>>();
		
		String tokens[] = inputStr.split(delimiter);
		
		m_clusterId = Integer.parseInt(tokens[0]);
		
		for(int i=1; i<tokens.length; i++)
		{
			int attrIndex = Integer.parseInt(tokens[i++]);
			String attrValue = tokens[i];
			
			String subTokens[] = attrValue.split(subDelimiter);
			if(CommonMethods.isNumeric(subTokens[0]))
			{
				PDF_Features value = new PDF_Features();
				value.average = Double.parseDouble(subTokens[0]);
				value.variance = Double.parseDouble(subTokens[1]);
			}
			else
			{
				HashMap<String, Double> valueMap = new HashMap<String, Double>();
				for(int k=0; k<subTokens.length; k++)
				{
					String subKey = subTokens[k++];
					double subValue = Double.parseDouble(subTokens[k]);
					valueMap.put(subKey, subValue);
				}
				m_nominalValueList.put(attrIndex, valueMap);
			}
		}
	}
	
	public static EMClusterStructure[] loadClusterInfoFile(Configuration conf, Path clusterPath, int clusterCnt, String demiliter) throws IOException
	{
		EMClusterStructure[] clusters = new EMClusterStructure[clusterCnt];
		
		FileStatus[] status = FileSystem.get(conf).listStatus(clusterPath);		
		for(int i=0; i<status.length; i++)
		{
			FSDataInputStream fin = FileSystem.get(conf).open(status[i].getPath());
			BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
			String readStr;
			while((readStr = br.readLine())!=null)
			{	
				int clusterId = Integer.parseInt(readStr.substring(0, readStr.indexOf(demiliter)));
				clusters[clusterId] = new EMClusterStructure();
				clusters[clusterId].loadClusterInfoString(readStr, demiliter, conf.get("subDelimiter", "@@"));
			}
			br.close();
			fin.close();
		}
		
		return clusters;
	}
	
	
	
	public boolean isEqualClusterInfo(EMClusterStructure cluster)
	{
		Iterator<Integer> numericAttrIndexIter = m_numericValueList.keySet().iterator();		
		while(numericAttrIndexIter.hasNext())
		{
			int attrIndex = numericAttrIndexIter.next();
			
			if(!cluster.m_numericValueList.containsKey(attrIndex)) return false;			
			else if(!m_numericValueList.get(attrIndex).equals(cluster.m_numericValueList.get(attrIndex))) return false;
		}
		
		
		Iterator<Integer> nominalAttrIndexIter = m_nominalValueList.keySet().iterator();		
		while(nominalAttrIndexIter.hasNext())
		{	
			int attrIndex = nominalAttrIndexIter.next();
			
			if(!cluster.m_nominalValueList.containsKey(attrIndex)) return false;			
			else
			{
				HashMap<String, Double> valueMap = m_nominalValueList.get(attrIndex);
				HashMap<String, Double> clusterValueMap = cluster.m_nominalValueList.get(attrIndex);
				
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
