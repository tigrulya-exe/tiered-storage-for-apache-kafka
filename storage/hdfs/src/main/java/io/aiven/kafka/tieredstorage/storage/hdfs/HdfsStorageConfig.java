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

package io.aiven.kafka.tieredstorage.storage.hdfs;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HdfsStorageConfig extends AbstractConfig {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsStorageConfig.class);

    private static final ConfigDef CONFIG;

    static final String HDFS_CONF_PREFIX = "hdfs.conf.";

    static final String HDFS_ROOT_CONFIG = "hdfs.root";
    private static final String HDFS_ROOT_DEFAULT = "/";
    private static final String HDFS_ROOT_DOC =
        "The base directory path in hdfs relative to which all uploaded file paths are resolved ";

    static final String HDFS_UPLOAD_BUFFER_SIZE_CONFIG = "hdfs.upload.buffer.size";
    private static final int HDFS_UPLOAD_BUFFER_SIZE_DEFAULT = 8192;
    private static final String HDFS_UPLOAD_BUFFER_SIZE_DOC =
        "TODO";

    static final String HDFS_CORE_SITE_CONFIG = "hdfs.core-site.path";
    private static final String HDFS_CORE_SITE_DOC = "Path of core-site.xml";

    static final String HDFS_SITE_CONFIG = "hdfs.hdfs-site.path";
    private static final String HDFS_SITE_DOC = "Path of hdfs-site.xml";

    static {
        CONFIG = new ConfigDef()
            .define(
                HDFS_ROOT_CONFIG,
                ConfigDef.Type.STRING,
                HDFS_ROOT_DEFAULT,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                HDFS_ROOT_DOC
            )
            .define(
                HDFS_UPLOAD_BUFFER_SIZE_CONFIG,
                ConfigDef.Type.INT,
                HDFS_UPLOAD_BUFFER_SIZE_DEFAULT,
                ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.MEDIUM,
                HDFS_UPLOAD_BUFFER_SIZE_DOC
            )
            .define(
                HDFS_CORE_SITE_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                HDFS_CORE_SITE_DOC
            )
            .define(
                HDFS_SITE_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                HDFS_SITE_DOC
            );
    }

    private final Configuration hadoopConf;

    HdfsStorageConfig(final Map<String, ?> props) {
        super(CONFIG, removeHadoopConfProps(props));
        this.hadoopConf = buildHadoopConf(props);
    }

    final String rootDirectory() {
        return getString(HDFS_ROOT_CONFIG);
    }

    final int uploadBufferSize() {
        return getInt(HDFS_UPLOAD_BUFFER_SIZE_CONFIG);
    }

    final Configuration hadoopConf() {
        return hadoopConf;
    }

    private Configuration buildHadoopConf(final Map<String, ?> props) {
        final Configuration hadoopConf = new Configuration();

        props.entrySet()
            .stream()
            .filter(entry -> entry.getKey().contains(HDFS_CONF_PREFIX))
            .forEach(entry -> hadoopConf.set(
                entry.getKey().replaceFirst(HDFS_CONF_PREFIX, ""),
                entry.getValue().toString()
            ));

        LOG.info("<-------- BUILT CONFIG BEFORE RESOURCES: {}", hadoopConf.getPropsWithPrefix(""));

        addResourceIfPresent(hadoopConf, HDFS_CORE_SITE_CONFIG);
        addResourceIfPresent(hadoopConf, HDFS_SITE_CONFIG);

        LOG.info("<-------- BUILT CONFIG: {}", hadoopConf.getPropsWithPrefix(""));

        return hadoopConf;
    }

    private void addResourceIfPresent(final Configuration hadoopConf, final String configKey) {
        final String resource = getString(configKey);
        if (resource != null) {
            hadoopConf.addResource(new Path("file", null, resource));
        }
    }

    private static Map<String, ?> removeHadoopConfProps(final Map<String, ?> props) {
        return props.entrySet()
            .stream()
            .filter(entry -> !entry.getKey().contains(HDFS_CONF_PREFIX))
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue
            ));
    }
}
