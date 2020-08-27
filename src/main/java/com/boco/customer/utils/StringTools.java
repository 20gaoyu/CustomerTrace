package com.boco.customer.utils;

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.math.BigDecimal;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import eu.bitwalker.useragentutils.UserAgent;

public final class StringTools {
	private static String EMPTY = ""; // 默认返回的空字符

	// 匹配前27个(xxx|)组合
	private static Pattern pURL = Pattern.compile("([^\\|]*\\|){27}");
	// 匹配url参数--过时
	// private static Pattern pURLParam = Pattern.compile("[^:]*://[^/]*/?");
	// 匹配时间中的非数字
	private static Pattern pDate = Pattern.compile("[^\\d]");
	// 匹配IP地址
	private static Pattern pIP = Pattern.compile("^(?:\\d{1,3}\\.){3}\\d{1,3}");
	// 匹配非数字开头
	private static Pattern pNum = Pattern.compile("^[^\\d].*");

	// 随机数
	private static Random random = new Random();
	// 随机数长度
	private final static int RANDOM_COUNT = 6;
	private final static int RANDOM_BASE = 100000;
	private static BigDecimal bigDecimal;

	/**
	 * 根据正则获取第28个字段 URL reg=(?:[^\|]*?\|){27} 去掉27个匹配的(xxx|)组合
	 * 
	 * @param str
	 * @param pattern
	 * @return
	 */
	public static String getURL(String str) {
		if (isNotBlank(str)) {
			return pURL.matcher(str).replaceFirst("");
		}
		return EMPTY;
	}

	/**
	 * 取得非URL数据 前27个字段
	 * 
	 * @param allData
	 * @param strURL
	 * @return
	 */
	public static String getNotURL(String allData, String strURL) {
		if (isNotBlank(allData)) { // 添加空格,防止split之后数组变短
			return StringUtils.removeEnd(allData, strURL) + " ";
		}
		return EMPTY;
	}

	/**
	 * 截取start_time前19位
	 * 
	 * @param strIMEI
	 * @return
	 */
	public static String subStartTime19(String strIMEI) {
		return isNotBlank(strIMEI) ? StringUtils.substring(strIMEI, 0, 19) : EMPTY;
	}

	/**
	 * 截取IEMI前8位
	 * 
	 * @param strIMEI
	 * @return
	 */
	public static String subIMEI8(String strIMEI) {
		return isNotBlank(strIMEI) ? StringUtils.substring(strIMEI, 0, 8) : EMPTY;
	}

	/**
	 * 截取URL后面的参数 需求是：截取域名之后的路径和参数
	 * 
	 * @param strURL
	 * @return
	 */
	public static String replaceAllUrlParam(String strURL) {
		if (isNotBlank(strURL)) {
			return StringUtils.substringAfter(strURL, "?");
		}
		return EMPTY;
	}

	/**
	 * 根据url提取协议 协议必须包含 :// 去掉协议是数字的
	 * 
	 * @param url
	 * @return
	 */
	public static String getProtocol(String url) {
		if (isNotBlank(url.trim()) && url.indexOf("://") > -1) {
			String str = StringUtils.substringBefore(url, ":").trim();
			return !StringUtils.isNumeric(str) ? str.toUpperCase() : EMPTY;
		}
		return EMPTY;
	}

	/**
	 * 获取域名 域名中必须包含"."
	 * 
	 * @param strHost
	 * @return
	 */
	public static String getHost(String strHost) {
		if (isNotBlank(strHost)) {
			return strHost.indexOf(".") > -1 ? strHost : EMPTY;
		}
		return EMPTY;
	}

	/**
	 * 获取content_type 非数字开头 截取"/"之前的值
	 * 
	 * @param str
	 * @return
	 */
	public static String getContentType(String str) {
		if (isNotBlank(str)) {
			return pNum.matcher(str).matches() ? StringUtils.substringBefore(str, "/") : EMPTY;
		}
		return EMPTY;
	}

	/**
	 * 获取ip ip必须符合规则
	 * 
	 * @param strIp
	 * @return
	 */
	public static String getIp(String strIp) {
		if (isNotBlank(strIp)) {
			return pIP.matcher(strIp).matches() ? strIp : EMPTY;
		}
		return EMPTY;
	}

	/**
	 * 通过USER_AGENT获取浏览器
	 * 
	 * @param userAgent
	 * @return
	 */
	public static String getBrowse(String userAgent) {
		if (isNotBlank(userAgent)) {
			return StringUtils.substringBefore(userAgent, "/");
		}
		return EMPTY;
	}

	/**
	 * 将日期处理成业务所需key
	 * 
	 * @param strDate
	 *            格式：2013-03-20 16:00:52.876217
	 * @return 201303201600
	 */
	public static String getDateKey(String strDate, int... count) {
		if (isNotBlank(strDate)) {
			String strTemp = pDate.matcher(strDate).replaceAll("");
			if (count.length > 0) {
				return StringUtils.substring(strTemp, 0, count[0]);
			} else {
				return StringUtils.substring(strTemp, 0, 12);
			}
		}
		return EMPTY;
	}

	/**
	 * 字符串拼接 先计算出字符串长度 声明此长度的stringbuilder
	 * 
	 * @param strs
	 * @return
	 */
	public static String append(Object... objs) {
		if (0 < objs.length) {
			return StringUtils.join(objs);
		}
		return EMPTY;
	}

	/**
	 * 用strChar为分隔符拼接
	 * 
	 * @param strChar
	 * @param objs
	 * @return
	 */
	public static String join(String strChar, Object... objs) {
		if (objs.length > 0) {
			return StringUtils.join(objs, strChar);
		}
		return EMPTY;
	}

	/**
	 * 根据userAgent解析出浏览器
	 * 
	 * @param userAgent
	 * @return
	 */
	public static String parseBrowse(UserAgent u) {
		if (u != null) {
			return u.getBrowser().getName();
		}
		return EMPTY;
	}

	/**
	 * 根据userAgent解析出操作系统
	 * 
	 * @param userAgent
	 * @return
	 */
	public static String parseOS(UserAgent u) {
		if (u != null) {
			return u.getOperatingSystem().getName();
		}
		return EMPTY;
	}

	/**
	 * 生成十位随机数
	 * 
	 * @return
	 */
	public static String getRandom() {
		return StringUtils.rightPad(String.valueOf(random.nextInt(RANDOM_BASE)), RANDOM_COUNT, "0");
	}

	public static void main(String[] args) {
	}

	/**
	 * 显示float型的数据时不以科学计数法
	 * 
	 * @param obj
	 */
	public static String nonScientificNotation(float f) {
		bigDecimal = new BigDecimal(f);
		bigDecimal = bigDecimal.setScale(6, BigDecimal.ROUND_HALF_UP);
		return bigDecimal.toString();
	}

	public static String nonScientificNotationStr(String s) {
		bigDecimal = new BigDecimal(s);
		bigDecimal = bigDecimal.setScale(6, BigDecimal.ROUND_HALF_UP);
		return bigDecimal.toString();
	}

	/**
	 * 将Y->1，N->0
	 * 
	 */
	public static String tansform(String s) {
		if (s.equals("Y"))
			return "1";
		else if (s.equals("N"))
			return "0";
		else {
			return null;
		}
	}
}
