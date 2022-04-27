package com.alecuba16.cdap;

import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.*;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Batch Aggregator Plugin
 * A BatchAggregator plugin is used to compute aggregates over a batch of data. It is used in both batch and real-time
 * data pipelines. An aggregation takes place in two steps: groupBy and then aggregate. In the groupBy step, the
 * aggregator creates zero or more group keys for each input record. Before the aggregate step occurs, the CDAP pipeline
 * will take all records that have the same group key, and collect them into a group. If a record does not have any of
 * the group keys, it is filtered out. If a record has multiple group keys, it will belong to multiple groups.
 *
 * The aggregate step is then called. In this step, the plugin receives group keys and all records that had that group
 * key. It is then left to the plugin to decide what to do with each of the groups. In order to implement a Batch
 * Aggregator, you extend the BatchAggregator class. Unlike a Transform, which operates on a single record at a time,
 * a BatchAggregator operates on a collection of records.
 * Methods
 * configurePipeline(): Used to perform any validation on the application configuration that is required by this plugin
 * or to create any datasets if the fieldName for a dataset is not a macro.
 *
 * initialize(): Initialize the Batch Aggregator. Guaranteed to be executed before any call to the plugin’s groupBy or
 * aggregate methods. This is called by each executor of the job. For example, if the MapReduce engine is being used,
 * each mapper will call this method.
 *
 * destroy(): Destroy any resources created by initialize. Guaranteed to be executed after all calls to the plugin’s
 * groupBy or aggregate methods have been made. This is called by each executor of the job. For example, if the
 * MapReduce engine is being used, each mapper will call this method.
 *
 * groupBy(): This method will be called for every object that is received from the previous stage. This method returns
 * zero or more group keys for each object it receives. Objects with the same group key will be grouped together for
 * the aggregate method.
 * aggregate(): The method is called after every object has been assigned their group keys. This method is called once
 * for each group key emitted by the groupBy method. The method receives a group key as well as an iterator over all
 * objects that had that group key. Objects emitted in this method are the output for this stage.
 /**
 * Aggregator that counts how many times each word appears in records input to the aggregator.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name(WordCountAggregator.NAME)
@Description("Counts how many times each word appears in all records input to the aggregator.")
public class WordCountAggregator extends BatchAggregator<String, StructuredRecord, StructuredRecord> {
    public static final String NAME = "WordCount";
    public static final Schema OUTPUT_SCHEMA = Schema.recordOf(
            "wordCount",
            Schema.Field.of("word", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("count", Schema.of(Schema.Type.LONG))
    );
    private static final Pattern WHITESPACE = Pattern.compile("\\s");
    private final Conf config;

    /**
     * Config properties for the plugin.
     */
    public static class Conf extends PluginConfig {
        @Description("The field from the input records containing the words to count.")
        private String field;
    }

    public WordCountAggregator(Conf config) {
        this.config = config;
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        // any static configuration validation should happen here.
        // We will check that the field is in the input schema and is of type string.
        Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
        // a null input schema means its unknown until runtime, or its not constant
        if (inputSchema != null) {
            // if the input schema is constant and known at configure time, check that the input field exists and is a string.
            Schema.Field inputField = inputSchema.getField(config.field);
            if (inputField == null) {
                throw new IllegalArgumentException(
                        String.format("Field '%s' does not exist in input schema %s.", config.field, inputSchema));
            }
            Schema fieldSchema = inputField.getSchema();
            Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
            if (fieldType != Schema.Type.STRING) {
                throw new IllegalArgumentException(
                        String.format("Field '%s' is of illegal type %s. Must be of type %s.",
                                config.field, fieldType, Schema.Type.STRING));
            }
        }
        // set the output schema so downstream stages will know their input schema.
        pipelineConfigurer.getStageConfigurer().setOutputSchema(OUTPUT_SCHEMA);
    }

    @Override
    public void groupBy(StructuredRecord input, Emitter<String> groupKeyEmitter) throws Exception {
        String val = input.get(config.field);
        if (val == null) {
            return;
        }

        for (String word : WHITESPACE.split(val)) {
            groupKeyEmitter.emit(word);
        }
    }

    @Override
    public void aggregate(String groupKey, Iterator<StructuredRecord> groupValues,
                          Emitter<StructuredRecord> emitter) throws Exception {
        long count = 0;
        while (groupValues.hasNext()) {
            groupValues.next();
            count++;
        }
        emitter.emit(StructuredRecord.builder(OUTPUT_SCHEMA).set("word", groupKey).set("count", count).build());
    }
}