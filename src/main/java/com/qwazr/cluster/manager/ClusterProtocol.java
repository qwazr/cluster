/**
 * Copyright 2015-2016 Emmanuel Keller / QWAZR
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
package com.qwazr.cluster.manager;

import java.io.*;
import java.util.Set;
import java.util.UUID;

enum ClusterProtocol {

	join('J', FullContent.class),
	notify('N', AddressContent.class),
	forward('F', FullContent.class),
	reply('R', FullContent.class),
	alive('A', AddressContent.class),
	leave('L', AddressContent.class);

	final static String CHAR_HEADER = "QWAZR";

	final char cmd;
	final Class<? extends Externalizable> messageClass;

	ClusterProtocol(final char cmd, final Class<? extends Externalizable> messageClass) {
		this.cmd = cmd;
		this.messageClass = messageClass;
	}

	final static ClusterProtocol findCommand(final char cmd) {
		for (ClusterProtocol command : values())
			if (command.cmd == cmd)
				return command;
		throw new IllegalArgumentException("Command not found: " + cmd);
	}

	final static MessageContent newJoin(final String address, final UUID nodeLiveId, final Set<String> groups,
			final Set<String> services) {
		return new MessageContent(join, new FullContent(address, nodeLiveId, groups, services));
	}

	final static MessageContent newNotify(final AddressContent address) {
		return new MessageContent(notify, address);
	}

	final static MessageContent newForward(final String address, final UUID nodeLiveId, final Set<String> groups,
			final Set<String> services) {
		return new MessageContent(forward, new FullContent(address, nodeLiveId, groups, services));
	}

	final static MessageContent newReply(final String address, final UUID nodeLiveId, final Set<String> groups,
			final Set<String> services) {
		return new MessageContent(reply, new FullContent(address, nodeLiveId, groups, services));
	}

	final static MessageContent newAlive(final String address, final UUID nodeLiveId) {
		return new MessageContent(alive, new AddressContent(address, nodeLiveId));
	}

	final static MessageContent newLeave(final String address, final UUID nodeLiveId) {
		return new MessageContent(leave, new AddressContent(address, nodeLiveId));
	}

}
