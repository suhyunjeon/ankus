# What is Ankus?  

November 29, 2013  
We released ankus 0.1 (branch : new_ankus-0.1)  


## Official Release
ankus 0.1 - current stable version  
ankus 0.0.1 - first stable version but missing ID3, EM, Content based Similarity modules.  


## New features of ankus 0.1  
1) Classification - ID3  
2) Clustering - EM  
3) Similarity - Content based Similarity  
4) Recommendation System - Item based recommendation  
5) Recommendation verify module(use RMSE)  


Ankus is an open source data mining / machine learning based MapReduce that supports a variety of advanced algorithms. Apache Mahout have the same goal with us, Mahout complicated convert to Sequence files and configure parameters for a wide variety of machine learning algorithms. But as Ankus can see below, Almost do not need to generate input dataset POV(Point of view) analysis as set up a variety of custom parameters Focus on the pre-processing as normalization dataset

OUR GOAL is, machine learning and data mining library on top of Apache Hadoop using the map/reduce paradigm. And they are an open source project.

## Supoort algorithms

1) Basic statistics computation for numeric/nominal data (3 methods)  
2) Pre-processing (Normalization, 1 method)  
3) Similarity/correlation analysis for vector type data (3 methods)  
4) Classification/clustering analysis (3 methods)  
5) CF based recommendation analysis (4 methods)  

## Feautures

1) Can use without input-file conversion  
2) Support various parameters for algorithms  
3) Support basic statistics and pre-processing methods  
4) Support attributes selection for analysis  

## Architecture  
![Alt text](http://www.openankus.org/download/attachments/1736818/image2013-7-11%209-31-24.png?version=1&modificationDate=1375342093394&api=v2 "Ankus architecture")


## Community

Join community forum! https://www.facebook.com/groups/openankus

Join facebook page! https://www.facebook.com/openankus

See! It's wiki! (Manual and more detail) http://www.openankus.org/

Only download jar files https://sourceforge.net/projects/ankus/files/?source=navbar

Demo video http://youtu.be/gx8i4X82QfQ

## License
Apache License 2.0  


## For Korean
Ankus는 Hadoop MapReduce 기반 환경에서 운용할 수 있는 데이터 마이닝/기계학습 라이브러리 입니다. 
Apache Mahout과 동일한 목적이나 Mahout은 Sequence 파일로의 변환과 다양한 분석 실험을 위한 파라미터들의 설정이 복잡하고, 접근방법이 어렵습니다.
반면 Ankus는 분석 수행 관점에서 아래와 같이 사용이 가능합니다.  
1) 입력 파일을 별도의 변환 없이 그대로 사용 가능  
2) 다양한 파라미터들을 설정하여 여러 관점에서 분석 가능  
3) 정규화 같은 입력 값의 전처리 등을 수행 할 수 있도록 하는데 더 중점을 둠  

빅데이터 환경에서 그동안 어려웠던 마이닝/기계학습 분석을  더욱 쉽게 분석해볼 수 있는 오픈소스 라이브러리입니다.  
