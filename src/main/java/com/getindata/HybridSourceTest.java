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

package com.getindata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;
import java.time.Duration;

public class HybridSourceTest {

    private static final String ONE_ORDERED = "test-data/test-data-one-ordered-file/";
    private static final String ONE_UNORDERED = "test-data/test-data-one-unordered-file/";
    private static final String TWO_ORDERED = "test-data/test-data-two-ordered-files/";
    private static final String MANY_ORDERED = "test-data/test-data-multiple-ordered-2min-files";

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString("table.exec.mini-batch.enabled", "true");
        configuration.setString("table.exec.mini-batch.allow-latency", "2s");
        configuration.setString("table.exec.mini-batch.size", "5000");
        configuration.setString("pipeline.auto-watermark-interval", "1ms");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);
        final StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        FileSource<TestData> fileSource =
                FileSource.forRecordStreamFormat(
                                new TestDataFormat(),
                                // TODO: Change path depending on your test scenario
                                Path.fromLocalFile(new File(ONE_ORDERED))
                        )
                        .setSplitAssigner((FileSplitAssigner.Provider) OrderedFileSplitAssigner::new)
                        .build();

        KafkaSource<TestData> kafkaSource =
                KafkaSource.<TestData>builder()
                        .setBootstrapServers("localhost:29092")
                        .setTopics("input")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new JsonDeserializationSchema<>(TestData.class))
                        .build();

        HybridSource<TestData> hybridSource =
                HybridSource.builder(fileSource)
                        .addSource(kafkaSource)
                        .build();

        WatermarkStrategy<TestData> watermarkStrategy = WatermarkStrategy
                .<TestData>forBoundedOutOfOrderness(Duration.ofSeconds(120))
                .withTimestampAssigner((el, rt) -> el.getTs().toEpochMilli());

        SingleOutputStreamOperator<TestData> step = env
                .fromSource(hybridSource, watermarkStrategy, "hybrid-source").returns(TestData.class)
                .map((MapFunction<TestData, TestData>) value -> {
                    Thread.sleep(5L);
                    return value;
                });

        Table table = tenv.fromDataStream(
                step,
                Schema.newBuilder()
                        .columnByExpression("rowtime", "CAST(ts AS TIMESTAMP_LTZ(3))")
                        .watermark("rowtime", "rowtime - INTERVAL '2' MINUTES")
                        .columnByExpression("proctime", "proctime()")
                        .build()
        );

        tenv.createTemporaryView("test_data", table);
        table.printSchema();
        tenv.executeSql("DESCRIBE test_data").print();
        tenv.executeSql(
                "SELECT * " +
                        "FROM ( " +
                        "  SELECT " +
                        "    userId, " +
                        "    SUM(`value`) OVER ( " +
                        "      PARTITION BY userId " +
                        "      ORDER BY rowtime ASC " +
                        "      RANGE BETWEEN INTERVAL '5' MINUTES PRECEDING AND CURRENT ROW " +
                        "    ) AS sum_5_min " +
                        "  FROM test_data" +
                        ") " +
                        "WHERE sum_5_min > 100"
        ).print();
        env.execute("Hybrid Source Test");
    }
}
