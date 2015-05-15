/**
 * Copyright 2015 Emmanuel Keller / QWAZR
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwazr.cluster.manager;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class ClusterConfiguration {

	public final Set<String> masters;

	public ClusterConfiguration() {
		masters = null;
	}

	public static ClusterConfiguration newInstance(File clusterConfigurationFile)
			throws IOException {
		if (!clusterConfigurationFile.exists()
				|| clusterConfigurationFile.length() == 0)
			return null;
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		return mapper.readValue(clusterConfigurationFile,
				ClusterConfiguration.class);
	}

}
