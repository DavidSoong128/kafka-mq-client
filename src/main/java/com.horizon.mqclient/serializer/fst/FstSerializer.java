package com.horizon.mqclient.serializer.fst;

import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
/**
 * serialize tool based on Fst
 * @author : David.Song/Java Engineer
 * @date : 2016/1/4 11:41
 * @since : 1.0.0
 */
public class FstSerializer implements Serializer{

	private Logger logger = LoggerFactory.getLogger(FstSerializer.class);

	private FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

	public byte[] serialize(Object object) throws IOException {
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		FSTObjectOutput out = conf.getObjectOutput(stream);
		out.writeObject(object);
		out.flush();
		stream.close();
		return stream.toByteArray();
	}

	@Override
	public <T> T deserialize(byte[] bytes, Class<T> clazz) throws IOException {
		T result = null;
		try {
			ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
			FSTObjectInput in = conf.getObjectInput(stream);
			result = (T) in.readObject(clazz);
			stream.close();
		} catch (Exception e) {
			logger.error("",e);
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	public <T> T deserialize(byte[] bytes) throws IOException {
		T result = null;
		try {
			ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
			FSTObjectInput in = conf.getObjectInput(stream);
			result = (T) in.readObject();
			stream.close();
		} catch (Exception e) {
			logger.error("",e);
		}
		return result;
	}
}
