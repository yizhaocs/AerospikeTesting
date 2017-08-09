package com.adara.newcache.utils;
public class UuidGenerator {
	public static void main(String[] args) {
		System.out.println(generateRandomUuid()); // 2e386b19-8727-4f73-b913-805fe49f8c56
		System.out.println(generateUuidBytes("1".getBytes())); // c4ca4238-a0b9-3382-8dcc-509a6f75849b
	}

	public static String generateRandomUuid() {
		return java.util.UUID.randomUUID().toString();
	}

	public static String generateUuidBytes(byte[] byteArray) {
		return java.util.UUID.nameUUIDFromBytes(byteArray).toString();
	}
}
