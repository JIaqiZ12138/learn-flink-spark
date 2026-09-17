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

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 从容器工作目录下的文件里把 JobGraph 反序列化出来。
 *
 * <p>per-job 模式的专用实现：客户端把 JobGraph 序列化成 {@code job.graph}（文件名由
 * {@link #JOB_GRAPH_FILE_PATH} = {@code internal.jobgraph-path} 指定，默认 "job.graph"），
 * 随 YARN local resource 分发到 AM 容器；AM 起来后由本类读回。
 *
 * <p>注意除了反序列化，它还会调 {@code addUserClassPathsToJobGraph()} 把容器里
 * {@code usrlib} 目录的路径合并进 {@code JobGraph.classpaths}——因为 JobGraph 是在
 * <b>客户端</b>生成的，那时还不知道集群侧的 usrlib 在哪。
 *
 * <p>{@link JobGraphRetriever} implementation which retrieves the {@link JobGraph} from a file on
 * disk.
 */
public class FileJobGraphRetriever extends AbstractUserClassPathJobGraphRetriever {

    @Internal
    public static final ConfigOption<String> JOB_GRAPH_FILE_PATH =
            ConfigOptions.key("internal.jobgraph-path").stringType().defaultValue("job.graph");

    @Nonnull private final String jobGraphFile;

    public FileJobGraphRetriever(@Nonnull String jobGraphFile, @Nullable File usrLibDir)
            throws IOException {
        super(usrLibDir);
        this.jobGraphFile = jobGraphFile;
    }

    @Override
    public JobGraph retrieveJobGraph(Configuration configuration) throws FlinkException {
        final File fp = new File(jobGraphFile);

        try (FileInputStream input = new FileInputStream(fp);
                ObjectInputStream obInput = new ObjectInputStream(input)) {
            final JobGraph jobGraph = (JobGraph) obInput.readObject(); // 反序列化
            addUserClassPathsToJobGraph(jobGraph);
            return jobGraph;
        } catch (FileNotFoundException e) {
            throw new FlinkException("Could not find the JobGraph file.", e);
        } catch (ClassNotFoundException | IOException e) {
            throw new FlinkException("Could not load the JobGraph from file.", e);
        }
    }

    private void addUserClassPathsToJobGraph(JobGraph jobGraph) {
        final List<URL> classPaths = new ArrayList<>();

        if (jobGraph.getClasspaths() != null) {
            classPaths.addAll(jobGraph.getClasspaths());
        }
        classPaths.addAll(getUserClassPaths());
        jobGraph.setClasspaths(classPaths);
    }

    public static FileJobGraphRetriever createFrom(
            Configuration configuration, @Nullable File usrLibDir) throws IOException {
        checkNotNull(configuration, "configuration");
        return new FileJobGraphRetriever(configuration.get(JOB_GRAPH_FILE_PATH), usrLibDir);
    }
}
