package com.horizon.kafka.client.serializer.fst;

import java.io.IOException;

/**
 *@author : David.Song
 *@date : 2015年11月30日 下午6:21:52
 *@since : 1.0.0
 *@see
 */
public interface Serializer {

	public byte[] serialize(Object object) throws IOException;

	public <T> T deserialize(byte[] bytes, Class<T> clazz) throws IOException;

	public <T> T deserialize(byte[] bytes) throws IOException;
}
