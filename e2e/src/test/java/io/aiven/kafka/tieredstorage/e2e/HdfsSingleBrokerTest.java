/*
 * Copyright 2023 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.tieredstorage.e2e;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class HdfsSingleBrokerTest extends SingleBrokerTest {
    private static final int NAME_NODE_PORT = 9870;
    static final String NAME_NODE_NETWORK_ALIAS = "namenode";

    static final Map<String, String> HADOOP_CONFIG = Map.of(
        "fs.default.name", "hdfs://namenode",
        "fs.defaultFs", "hdfs://namenode",
        "dfs.namenode.rpc-address", "namenode:8020",
        "dfs.replication", "1"
    );

    private static FileSystem fileSystem;

    static final GenericContainer<?> HDFS_NAMENODE_CONTAINER =
        new GenericContainer<>(DockerImageName.parse("apache/hadoop:3.2.4"))
            .withExposedPorts(NAME_NODE_PORT)
            .withCommand("hdfs namenode")
            .withNetwork(NETWORK)
            .withNetworkAliases(NAME_NODE_NETWORK_ALIAS)
            .withEnv(HADOOP_CONFIG);

    static final GenericContainer<?> HDFS_DATANODE_CONTAINER =
        new GenericContainer<>(DockerImageName.parse("apache/hadoop:3.2.4"))
            .withCommand("hdfs datanode")
            .withNetwork(NETWORK)
            .withEnv(HADOOP_CONFIG);

    @BeforeAll
    static void init() throws Exception {
        HDFS_NAMENODE_CONTAINER.start();
        HDFS_DATANODE_CONTAINER.start();

        setupKafka(HdfsSingleBrokerTest::setupKafka);

        final Configuration hadoopConf = new Configuration();
        HADOOP_CONFIG.forEach(hadoopConf::set);
        fileSystem = FileSystem.get(hadoopConf);
    }

    private static void setupKafka(final KafkaContainer kafkaContainer) {
        kafka.withEnv("KAFKA_RSM_CONFIG_STORAGE_BACKEND_CLASS",
                "io.aiven.kafka.tieredstorage.storage.hdfs.HdfsStorage")
            .withEnv("KAFKA_REMOTE_LOG_STORAGE_MANAGER_CLASS_PATH",
                "/tiered-storage-for-apache-kafka/core/*:/tiered-storage-for-apache-kafka/hdfs/*")
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_HDFS_ROOT", "/tmp/test/")
            .dependsOn(HDFS_NAMENODE_CONTAINER, HDFS_DATANODE_CONTAINER);

        HADOOP_CONFIG.forEach((key, value) -> kafka.withEnv(
            "KAFKA_RSM_CONFIG_STORAGE_HDFS_CONF_" + key,
            value
        ));
    }

    @AfterAll
    static void cleanup() {
        stopKafka();

        HDFS_DATANODE_CONTAINER.stop();
        HDFS_NAMENODE_CONTAINER.stop();

        cleanupStorage();
    }

    @Override
    boolean assertNoTopicDataOnTierStorage(final String topicName, final Uuid topicId) {
        final String prefix = String.format("%s-%s", topicName, topicId.toString());

        try {
            final FileStatus[] fileStatuses = fileSystem.listStatus(
                new Path("/"), path -> path.toUri().getPath().startsWith(prefix));
            return fileStatuses.length == 0;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    List<String> remotePartitionFiles(final TopicIdPartition topicIdPartition) {
        final String partitionDirRelativePath = String.format("%s-%s/%s",
            topicIdPartition.topic(),
            topicIdPartition.topicId().toString(),
            topicIdPartition.partition());
        try {
            return listFiles(partitionDirRelativePath)
                .map(FileStatus::getPath)
                .map(Object::toString)
                .sorted()
                .collect(Collectors.toList());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Stream<FileStatus> listFiles(final String directory) throws IOException {
        final Path directoryPath = new Path(directory);
        return Arrays.stream(fileSystem.listStatus(directoryPath));
    }
}
