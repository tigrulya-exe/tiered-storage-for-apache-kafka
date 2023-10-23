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

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

class HdfsStorageConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;

    private static final String ROOT_CONFIG = "root";
    private static final String ROOT_DOC = "Root directory";

    private static final String CORE_SITE_CONFIG = "core.site.path";
    private static final String CORE_SITE_DOC = "Path of core-site.xml";

    private static final String HDFS_SITE_CONFIG = "hdfs.site.path";
    private static final String HDFS_SITE_DOC = "Path of hdfs-site.xml";

    static {
        CONFIG = new ConfigDef()
                .define(
                        ROOT_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        ROOT_DOC
                )
                .define(
                        CORE_SITE_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        CORE_SITE_DOC
                )
                .define(
                        HDFS_SITE_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        HDFS_SITE_DOC
                );
    }

    HdfsStorageConfig(final Map<String, ?> props) {
        super(CONFIG, props);
    }

    final String rootDirectory() {
        return getString(ROOT_CONFIG);
    }

    Configuration toHadoopConf() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.addResource(getString(CORE_SITE_CONFIG));
        hadoopConf.addResource(getString(HDFS_SITE_CONFIG));
        return hadoopConf;
    }
}
