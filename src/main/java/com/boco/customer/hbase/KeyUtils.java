package com.boco.customer.hbase;

import java.util.Random;

public class KeyUtils {

	/**
	 * 手机号码、天、小时、八位随机数生成key
	 * 
	 * @param msisdn
	 * @param day
	 * @param hour
	 * @return
	 */
	public static byte[] buildKey(String msisdn, String dateTime) {
		String key = reverse(msisdn) + dateTime + randomStr();
		byte[] b = key.getBytes();
		return b;
	}
	public static byte[] buildKey(String msisdn, String day, String hour) {
		String key = reverse(msisdn) + day + hour + randomStr();
		byte[] b = key.getBytes();
		return b;
	}
	/**
	 * 实现字符串反转
	 * 
	 * @param s
	 * @return
	 */
	public static String reverse(String s) {
		char[] array = s.toCharArray();
		String reverse = "";
		for (int i = array.length - 1; i >= 0; i--)
			reverse += array[i];
		return reverse;
	}

	/**
	 * 返回八位随机数
	 * 
	 * @return
	 */
	private static String randomStr() {
		Random random = new Random();
		int[] array = new int[4];
		String s = "";
		for (int i = 0; i < 4; i++) {
			array[i] = random.nextInt(10);
			s = s + array[i];
		}
		return s;
	}
}
