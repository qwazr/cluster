/**
 * Copyright 2014-2016 Emmanuel Keller / QWAZR
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwazr.cluster.test;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.Arrays;
import java.util.List;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MulticastTest {

	static TestServer master1;
	static TestServer master2;
	static TestServer front1;
	static TestServer front2;
	static TestServer front3;

	private static final List<String> MASTERS = Arrays.asList("http://localhost:9091", "http://localhost:9092");

	@Test
	public void test00_startInstances() throws Exception {
		master1 = new TestServer(MASTERS, 9091, null);
		master2 = new TestServer(MASTERS, 9092, null);
		front1 = new TestServer(MASTERS, 9093, null);
		front2 = new TestServer(MASTERS, 9094, null);
		front3 = new TestServer(MASTERS, 9095, null);
	}

	@Test
	public void test10_sleep() throws InterruptedException {
		Thread.sleep(30000);
	}
}
