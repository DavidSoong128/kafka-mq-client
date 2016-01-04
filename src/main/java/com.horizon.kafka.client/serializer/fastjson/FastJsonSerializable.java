package com.horizon.kafka.client.serializer.fastjson;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.nio.charset.Charset;
/**
 * serialize tool based on fastJson
 * @author : David.Song/Java Engineer
 * @date : 2016/1/4 11:41
 * @since : 1.0.0
 */
public class FastJsonSerializable {
	
	public String toJson() {
		return toJson(false);
	}

	public static String toJson(final Object obj) {
		return JSON.toJSONString(obj, SerializerFeature.WriteMapNullValue, SerializerFeature.PrettyFormat,
				SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteNonStringKeyAsString);
	}

	public static <T> T fromJson(String json, Class<T> classOfT) {
		return JSON.parseObject(json, classOfT);
	}

	public byte[] encode() {
		final String json = this.toJson();
		if (json != null) {
			return json.getBytes();
		}
		return null;
	}

	public static byte[] encode(final Object obj) {
		final String json = toJson(obj);
		if (json != null) {
			return json.getBytes(Charset.forName("UTF-8"));
		}
		return null;
	}

	public static <T> T decode(final byte[] data, Class<T> classOfT) {
		final String json = new String(data, Charset.forName("UTF-8"));
		return fromJson(json, classOfT);
	}
}
