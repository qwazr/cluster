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
package com.qwazr.cluster.service;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_EMPTY)
public class ClusterNodeRegisterJson {

	public final String address;
	public final Set<String> services;

	public ClusterNodeRegisterJson() {
		address = null;
		services = null;
	}

	public ClusterNodeRegisterJson(String address, Set<String> services) {
		this.address = address;
		this.services = services;
	}

	public ClusterNodeRegisterJson(String address, String... services) {
		this.address = address;
		this.services = new HashSet<String>(Arrays.asList(services));
	}

}
