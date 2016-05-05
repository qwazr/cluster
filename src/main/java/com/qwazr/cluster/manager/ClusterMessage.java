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
package com.qwazr.cluster.manager;

import com.datastax.driver.core.utils.UUIDs;

import java.io.*;
import java.util.UUID;

final class ClusterMessage implements Externalizable {

	private UUID uuid;
	private ClusterProtocol command;
	private Externalizable content;

	ClusterMessage(final ClusterProtocol command, final Externalizable content) {
		this.uuid = UUIDs.timeBased();
		this.command = command;
		this.content = content;
	}

	ClusterMessage(final byte[] bytes) throws IOException, ReflectiveOperationException {
		try (final ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
			try (final ObjectInputStream ois = new ObjectInputStream(bis)) {
				readExternal(ois);
			}
		}
	}

	final ClusterProtocol getCommand() {
		return command;
	}

	final <T extends Externalizable> T getContent() {
		return (T) content;
	}

	@Override
	final public void writeExternal(final ObjectOutput out) throws IOException {
		out.writeLong(uuid.getMostSignificantBits());
		out.writeLong(uuid.getLeastSignificantBits());
		out.writeChar(command.cmd);
		content.writeExternal(out);
	}

	@Override
	final public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
		uuid = new UUID(in.readLong(), in.readLong());
		command = ClusterProtocol.findCommand(in.readChar());
		try {
			content = command.messageClass.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new IOException(e);
		}
		content.readExternal(in);
	}
}
