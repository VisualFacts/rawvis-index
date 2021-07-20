# RawVis: In-situ Visual Analytics System [Backend]

RawVis is an open source data visualization system for in-situ visual exploration and analytics over big raw data. 
RawVis implements novel indexing schemes and adaptive processing techniques allowing users to perform efficient visual and analytics operations directly over the data files. 
RawVis provides real-time interaction, reporting
low response time, over large data files (e.g., more than 50G & 100M objects), using commodity hardware.


In RawVis, the user selects a raw file to visualize and analyze, the file is parsed and indexed on-the-fly, generating a
“crude” initial version of our index. The user, then, performs visual operations, which are translated to queries evaluated over the index.
Based on the user interaction, the index is adapted incrementally, adjusting its structure and updating statistics.


* RawVis Homepage: http://rawvis.net

* Online Tool Demo: [[Link]](http://rawviz.imsi.athenarc.gr/visualize/taxi)

* Video Presentation: [[Link]](https://vimeo.com/500596816)



</br>

## Datasets

### Synthetic Dataset Generator:

Build the JAR file:

```

./mvnw clean install


```

To generate the SYNTH10 dataset, run the experiments executable JAR file:

```

java -jar ./target/experiments.jar -c synth10 -out synth10.csv

```
For SYNTH50, run:

```

java -jar ./target/experiments.jar -c synth50 -out synth50.csv

```

## Queries
* SYNTH10: [[Link]](https://github.com/VisualFacts/rawvis-index/tree/master/queries/synth_10_queries)

* SYNTH50: [[Link]](https://github.com/VisualFacts/rawvis-index/tree/master/queries/synth_50_queries)

* TAXI: [[Link]](https://github.com/VisualFacts/rawvis-index/tree/master/queries/taxi_queries)

</br>


## Running Instructions

First, build the JAR file:

```

./mvnw clean install


```

To execute a sequence of queries, e.g. using the SYNTH10 dataset, run the following:

```

java -Xmx16G -jar ./target/experiments.jar -c timeQueries -csv synth10.csv -bounds 0:1000,0:1000 -rect 544:574,323:353 -xCol 0 -yCol 1 -measureCol 9 -groupBy 2 -filters 3:1000000000,4:1000000000 -catCols 2,3,4,5,6,7 -initMode $initMode -binCount 100 -catBudget 2 -objCount 100000000 -seqCount 100 -minShift 10 -maxShift 10 -minFilters 2 -maxFilters 2 -out results.csv

```

## RawVis Prototype Tool  


* Front-end Source Code: [[Link]](https://github.com/VisualFacts/RawVis)

* Online Demo: [[Link]](http://rawviz.imsi.athenarc.gr/visualize/taxi)

* Video Presentation: [[Link]](https://vimeo.com/500596816)

</br>


## Publications


* Bikakis N., Maroulis S., Papastefanatos G., Vassiliadis P.: In-Situ Visual Exploration over Big Raw Data, Information Systems, Elsevier, 2021  [[pdf]](https://www.nbikakis.com/papers/in_situ_big_data_visual_analytics_indexing_IS_2020.pdf)
 
* Maroulis S., Bikakis N., Papastefanatos G., Vassiliadis P., Vassiliou Y.: RawVis: A System for Efficient In-situ Visual Analytics, intl. conf. on Management of Data (ACM SIGMOD/PODS '21)  [[pdf]](https://www.nbikakis.com/papers/RawVis_A_System_for_Efficient_In-situ_Visual_Analytics_SIGMOD2021.pdf)
 
* Maroulis S., Bikakis N., Papastefanatos G., Vassiliadis P., Vasiliou Y.: Adaptive Indexing for In-situ Visual Exploration and Analytics, 23rd intl. Workshop on Design, Optimization, Languages and Analytical Processing of Big Data (DOLAP '21)    [[pdf]](https://www.nbikakis.com/papers/RawVis_Adaptive_Indexing_for_In-situ_Visual_Exploration_and_Analytics_DOLAP2021.pdf)
 
* Bikakis N., Maroulis S., Papastefanatos G., Vassiliadis P.: RawVis: Visual Exploration over Raw Data, 22nd european conf. on advances in databases & information systems (ADBIS 2018)    [[pdf]](http://www.nbikakis.com/papers/RawVis.Visual.Exploration.over.Big.Raw.Data.pdf)
