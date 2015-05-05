/**
 * Copyright 2014-2015 OpenSearchServer Inc.
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
package com.qwazr.cluster;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import javax.servlet.ServletException;
import javax.ws.rs.ApplicationPath;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.qwazr.cluster.manager.ClusterManager;
import com.qwazr.cluster.service.ClusterServiceImpl;
import com.qwazr.utils.server.AbstractServer;
import com.qwazr.utils.server.RestApplication;
import com.qwazr.utils.server.ServletApplication;

public class ClusterServer extends AbstractServer {

	public final static ServerDefinition serverDefinition = new ServerDefinition();
	static {
		serverDefinition.defaultWebServiceTcpPort = 9099;
		serverDefinition.mainJarPath = "qwazr-cluster.jar";
	}

	/**
	 * Set the listening host or IP address
	 */
	public final static Option CONF_OPTION = new Option("c", "conf", true,
			"The path to the configuration file");

	private File configurationFile = null;

	private ClusterServer() {
		super(serverDefinition);
	}

	@ApplicationPath("/")
	public static class ClusterApplication extends RestApplication {

		@Override
		public Set<Class<?>> getClasses() {
			Set<Class<?>> classes = super.getClasses();
			classes.add(ClusterServiceImpl.class);
			return classes;
		}
	}

	public static void load(AbstractServer server, File data_directory,
			File configurationFile, Set<Class<?>> classes) throws IOException {
		ClusterManager.load(server, data_directory, configurationFile);
		if (classes != null)
			classes.add(ClusterApplication.class);
	}

	@Override
	public void defineOptions(Options options) {
		super.defineOptions(options);
		options.addOption(CONF_OPTION);
	}

	@Override
	public void commandLine(CommandLine cmd) throws IOException {
		configurationFile = cmd.hasOption(CONF_OPTION.getOpt()) ? new File(
				cmd.getOptionValue(CONF_OPTION.getOpt())) : null;
	}

	@Override
	public void load() throws IOException {
		load(this, getCurrentDataDir(), configurationFile, null);
	}

	@Override
	public RestApplication getRestApplication() {
		return new ClusterApplication();
	}

	@Override
	protected ServletApplication getServletApplication() {
		return null;
	}

	public static void main(String[] args) throws IOException, ParseException,
			ServletException {
		new ClusterServer().start(args);
	}

}
