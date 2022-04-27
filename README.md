# datafusion_cdap_batch_aggregator_plugin
Complete example project to create a custom Google cloud datafusion (CDAP) batch aggregator plugin. Sourced and adapted from the documentation where there is no quickstart project.

# Batch Aggregator Plugin
A BatchAggregator plugin is used to compute aggregates over a batch of data. It is used in both batch and real-time data pipelines. An aggregation takes place in two steps: groupBy and then aggregate.
In the groupBy step, the aggregator creates zero or more group keys for each input record. Before the aggregate step occurs, the CDAP pipeline will take all records that have the same group key, and collect them into a group. If a record does not have any of the group keys, it is filtered out. If a record has multiple group keys, it will belong to multiple groups.
The aggregate step is then called. In this step, the plugin receives group keys and all records that had that group key. It is then left to the plugin to decide what to do with each of the groups.
In order to implement a Batch Aggregator, you extend the BatchAggregator class. Unlike a Transform, which operates on a single record at a time, a BatchAggregator operates on a collection of records.

## Methods
### configurePipeline():
Used to perform any validation on the application configuration that is required by this plugin or to create any datasets if the fieldName for a dataset is not a macro.
### initialize():
Initialize the Batch Aggregator. Guaranteed to be executed before any call to the plugin’s groupBy or aggregate methods. This is called by each executor of the job. For example, if the MapReduce engine is being used, each mapper will call this method.
### destroy():
Destroy any resources created by initialize. Guaranteed to be executed after all calls to the plugin’s groupBy or aggregate methods have been made. This is called by each executor of the job. For example, if the MapReduce engine is being used, each mapper will call this method.
### groupBy():
This method will be called for every object that is received from the previous stage. This method returns zero or more group keys for each object it receives. Objects with the same group key will be grouped together for the aggregate method.
### aggregate():
The method is called after every object has been assigned their group keys. This method is called once for each group key emitted by the groupBy method. The method receives a group key as well as an iterator over all objects that had that group key. Objects emitted in this method are the output for this stage.