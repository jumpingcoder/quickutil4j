/**
 * IP/经纬度地理查询
 * 
 * @class GeoUtil
 * @author 0.5
 */

package com.quickutil.platform;

import java.io.File;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.model.CityResponse;
import com.quickutil.platform.def.GeoDef;
import com.quickutil.platform.def.GeoPoint;

public class GeoUtil {

	private static DatabaseReader databaseReader = null;
	private static Map<String, String> countryCodeByCountryNameMap = new HashMap<String, String>();
	private static Map<String, String> countryChineseByCountryCodeMap = new HashMap<String, String>();
	private static Map<String, String> stateNameByStateCodeMap = new HashMap<String, String>();
	private static Map<String, String> stateChineseByStateCodeMap = new HashMap<String, String>();
	private static Map<String, String> stateCodeByStateNameChineseMap = new HashMap<String, String>();
	private static Map<String, String> stateCodeByStateNameMap = new HashMap<String, String>();

	static {
		init();
	}

	public static void main(String[] args) {
		System.out.println(GeoIPByMMDB("117.136.68.137"));
		System.out.println(geoCodeyByBaidu(40.7, -74.0));
	}

	private static void init() {
		try {
			// 读取IP库
			String mmdbPath = FileUtil.getCurrentPath() + "/GeoIP2-City.mmdb";
			File mmdbFile = new File(mmdbPath);
			if (!mmdbFile.exists()) {
				HttpResponse response = HttpUtil.httpGet("http://quickutil.oss-cn-shenzhen.aliyuncs.com/GeoIP2-City.mmdb");
				byte[] mmdb = FileUtil.stream2byte(response.getEntity().getContent());
				if (mmdb != null)
					FileUtil.byte2File(mmdbPath, mmdb);
				mmdbFile = new File(mmdbPath);
			}
			databaseReader = new DatabaseReader.Builder(mmdbFile).build();
			// 读取国家地区库
			String countryStatePath = FileUtil.getCurrentPath() + "/country_state.json";
			File countryStateFile = new File(countryStatePath);
			if (!countryStateFile.exists()) {
				HttpResponse response = HttpUtil.httpGet("http://quickutil.oss-cn-shenzhen.aliyuncs.com/country_state.json");
				byte[] countryState = FileUtil.stream2byte(response.getEntity().getContent());
				if (countryState != null)
					FileUtil.byte2File(countryStatePath, countryState);
			}
			// 生成缓存
			List<Map<String, Object>> list = JsonUtil.toList(FileUtil.file2String(countryStatePath));
			for (Map<String, Object> map : list) {
				countryCodeByCountryNameMap.put((String) map.get("country_name"), (String) map.get("country_code"));
				countryChineseByCountryCodeMap.put((String) map.get("country_code"), (String) map.get("country_chinese"));
				stateNameByStateCodeMap.put((String) map.get("country_code") + "_" + (String) map.get("state_code"), (String) map.get("state_name"));
				stateChineseByStateCodeMap.put((String) map.get("country_code") + "_" + (String) map.get("state_code"), (String) map.get("state_chinese"));
				stateCodeByStateNameChineseMap.put((String) map.get("state_chinese"), (String) map.get("state_code"));
				stateCodeByStateNameMap.put((String) map.get("state_name"), (String) map.get("state_code"));
			}
			System.out.println("GeoUtil loaded successfully");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static String countryCodeByCountryName(String countryName) {
		return countryCodeByCountryNameMap.get(countryName);
	}

	private static String countryChineseByCountryCode(String countryCode) {
		return countryChineseByCountryCodeMap.get(countryCode);
	}

	private static String stateNameByStateCode(String countryCode, String stateCode) {
		return stateNameByStateCodeMap.get(countryCode + "_" + stateCode);
	}

	private static String stateChineseByStateCode(String countryCode, String stateCode) {
		return stateChineseByStateCodeMap.get(countryCode + "_" + stateCode);
	}

	private static String stateCodeByStateName(String stateName) {
		String stateCode = stateCodeByStateNameChineseMap.get(stateName);
		if (stateCode == null)
			stateCode = stateCodeByStateNameMap.get(stateName);
		if (stateCode == null)
			stateCode = stateName;
		return stateCode;
	}

	/**
	 * 根据IP查询地理信息
	 * 
	 * @param ip-ip地址
	 * @return
	 */
	private static final String UNKNOWN = "Unknown";

	public static GeoDef GeoIPByMMDB(String ip) {
		String countryCode = UNKNOWN;
		String country = UNKNOWN;
		String countryChinese = UNKNOWN;
		String stateCode = UNKNOWN;
		String state = UNKNOWN;
		String stateChinese = UNKNOWN;
		String city = UNKNOWN;
		Double latitude = 0.0;
		Double longitude = 0.0;
		try {
			InetAddress ipAddr = InetAddress.getByName(ip);
			CityResponse result;
			result = databaseReader.city(ipAddr);
			countryCode = result.getCountry().getIsoCode();
			country = result.getCountry().getName();
			countryChinese = countryChineseByCountryCode(countryCode);
			stateCode = result.getMostSpecificSubdivision().getIsoCode();
			state = result.getMostSpecificSubdivision().getName();
			stateChinese = stateChineseByStateCode(countryCode, stateCode);
			city = result.getCity().getName();
			if (countryCode.equals("CN"))
				city = result.getCity().getNames().get("zh-CN") + "市";
			latitude = result.getLocation().getLatitude();
			longitude = result.getLocation().getLongitude();
		} catch (AddressNotFoundException ae) {
		} catch (UnknownHostException ue) {
		} catch (Exception e) {
			e.printStackTrace();
		}
		countryCode = (countryCode == null) ? UNKNOWN : countryCode;
		country = (country == null) ? UNKNOWN : country;
		countryChinese = (countryChinese == null) ? UNKNOWN : countryChinese;
		stateCode = (stateCode == null) ? UNKNOWN : stateCode;
		state = (state == null) ? UNKNOWN : state;
		stateChinese = (stateChinese == null) ? UNKNOWN : stateChinese;
		city = (city == null) ? UNKNOWN : city;
		latitude = (latitude == null) ? 0.0 : latitude;
		longitude = (longitude == null) ? 0.0 : longitude;
		return new GeoDef(latitude, longitude, countryCode, country, countryChinese, stateCode, state, stateChinese, city, "");
	}

	/**
	 * 根据经纬度查询地理信息-百度世界
	 * 
	 * @param latitude-纬度
	 * @param longitude-经度
	 * @return
	 */

	public static GeoDef geoCodeyByBaidu(double latitude, double longitude) {
		try {
			double[] delta = WGSToGCJPointer(latitude, longitude);
			delta = GCJToBDPointer(delta[0], delta[1]);
			String apiKey = "RMVaRr172UQuTGVVTDKz1eGD";
			String queryUrl = String.format("http://api.map.baidu.com/geocoder/v2/?ak=%s&location=%s,%s&output=json", apiKey, delta[0], delta[1]);
			JsonObject object = JsonUtil.toJsonMap(FileUtil.stream2string(HttpUtil.httpGet(queryUrl).getEntity().getContent()));
			String country = object.getAsJsonObject("result").getAsJsonObject("addressComponent").get("country").getAsString();
			if (country.equals("中国"))
				country = "China";
			String countryCode = countryCodeByCountryName(country);
			String countryChinese = countryChineseByCountryCode(countryCode);
			String stateChinese = object.getAsJsonObject("result").getAsJsonObject("addressComponent").get("province").getAsString();
			String stateCode = stateCodeByStateName(stateChinese);
			String state = stateNameByStateCode(countryCode, stateCode);
			String city = object.getAsJsonObject("result").getAsJsonObject("addressComponent").get("city").getAsString();
			String description = object.getAsJsonObject("result").getAsJsonObject("addressComponent").get("district").getAsString();
			return new GeoDef(latitude, longitude, countryCode, country, countryChinese, stateCode, state, stateChinese, city, description);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 根据经纬度查询地理信息-百度批量
	 * 
	 * @param points-纬度经度数组，下标偶数位为纬度，奇数位为经度，必须成对，最多20条经纬度
	 * @return
	 */
	public static List<GeoDef> geoCodeyByBaidu(List<GeoPoint> points) {
		try {
			if (points.size() > 20)
				return null;
			StringBuilder builder = new StringBuilder();
			for (int i = 0; i < points.size(); i++) {
				double[] delta = WGSToGCJPointer(points.get(i).latitude, points.get(i).longitude);
				delta = GCJToBDPointer(delta[0], delta[1]);
				builder.append(delta[0] + "," + delta[1] + "|");
			}
			String apiKey = "ltXmyhH5stNNKDx1XDOsfUFIpCzAGRcT";
			String queryUrl = String.format("http://api.map.baidu.com/geocoder/v2/?ak=%s&location=%s&output=json&batch=true", apiKey,
					URLEncoder.encode(builder.substring(0, builder.length() - 1), "UTF-8"));
			JsonObject object = JsonUtil.toJsonMap(FileUtil.stream2string(HttpUtil.httpGet(queryUrl).getEntity().getContent()));
			JsonArray array = object.getAsJsonArray("areas");
			List<GeoDef> geodefList = new ArrayList<GeoDef>();
			for (int i = 0; i < points.size(); i++) {
				object = array.get(i).getAsJsonObject();
				String country = object.get("country").getAsString();
				if (country.equals("中国"))
					country = "China";
				String countryCode = countryCodeByCountryName(country);
				String countryChinese = countryChineseByCountryCode(countryCode);
				String stateChinese = object.get("province").getAsString();
				String stateCode = stateCodeByStateName(stateChinese);
				String state = stateNameByStateCode(countryCode, stateCode);
				String city = object.get("city").getAsString();
				String description = object.get("district").getAsString();
				geodefList.add(new GeoDef(points.get(i).latitude, points.get(i).longitude, countryCode, country, countryChinese, stateCode, state, stateChinese, city, description));
			}
			return geodefList;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 根据经纬度查询地理信息-高德中国
	 * 
	 * @param latitude-纬度
	 * @param longitude-经度
	 * @return
	 */
	public static GeoDef geoCodeyByAmap(double latitude, double longitude) {
		try {
			double[] delta = WGSToGCJPointer(latitude, longitude);
			String key = "b1501370e873f5784f75d43d061c181a";
			String queryUrl = String.format("http://restapi.amap.com/v3/geocode/regeo?output=json&location=%s,%s&key=%s", delta[1], delta[0], key);
			HttpResponse response = HttpUtil.httpGet(queryUrl);
			JsonObject object = JsonUtil.toJsonMap(FileUtil.stream2string(response.getEntity().getContent()));
			String country = object.getAsJsonObject("regeocode").getAsJsonObject("addressComponent").get("country").getAsString();
			String province = object.getAsJsonObject("regeocode").getAsJsonObject("addressComponent").get("province").getAsString();
			String city = province;
			if (!object.getAsJsonObject("regeocode").getAsJsonObject("addressComponent").get("city").isJsonArray())
				city = object.getAsJsonObject("regeocode").getAsJsonObject("addressComponent").get("city").getAsString();
			String description = object.getAsJsonObject("regeocode").get("formatted_address").getAsString();
			return new GeoDef(latitude, longitude, "", country, "", "", province, "", city, description);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * WGS-84 -> GCJ-02
	 * 
	 * @param latitude-纬度
	 * @param longitude-经度
	 * @return
	 */
	private static double[] WGSToGCJPointer(double latitude, double longitude) {
		if (outOfChina(latitude, longitude))
			return new double[] { latitude, longitude };
		double[] delta = delta(latitude, longitude);
		return new double[] { latitude + delta[0], longitude + delta[1] };
	}

	/**
	 * GCJ-02 -> BD-09
	 * 
	 * @param latitude-纬度
	 * @param longitude-经度
	 * @return
	 */
	private static double[] GCJToBDPointer(double latitude, double longitude) {
		double x = longitude;
		double y = latitude;
		double x_pi = 3.14159265358979324 * 3000.0 / 180.0;
		double z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * x_pi);
		double theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * x_pi);
		return new double[] { (z * Math.sin(theta) + 0.006), z * Math.cos(theta) + 0.0065 };
	}

	private static boolean outOfChina(double lat, double lng) {
		if (lng < 72.004 || lng > 137.8347) {
			return true;
		}
		if (lat < 0.8293 || lat > 55.8271) {
			return true;
		}
		return false;
	}

	private static double[] delta(double lat, double lng) {
		double[] delta = new double[2];
		double a = 6378137.0;
		double ee = 0.00669342162296594323;
		double dlat = transformLat(lng - 105.0, lat - 35.0);
		double dlng = transformLng(lng - 105.0, lat - 35.0);
		double radLat = lat / 180.0 * Math.PI;
		double magic = Math.sin(radLat);
		magic = 1 - ee * magic * magic;
		double sqrtMagic = Math.sqrt(magic);
		delta[0] = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * Math.PI);
		delta[1] = (dlng * 180.0) / (a / sqrtMagic * Math.cos(radLat) * Math.PI);
		return delta;
	}

	private static double transformLat(double x, double y) {
		double ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * Math.sqrt(Math.abs(x));
		ret += (20.0 * Math.sin(6.0 * x * Math.PI) + 20.0 * Math.sin(2.0 * x * Math.PI)) * 2.0 / 3.0;
		ret += (20.0 * Math.sin(y * Math.PI) + 40.0 * Math.sin(y / 3.0 * Math.PI)) * 2.0 / 3.0;
		ret += (160.0 * Math.sin(y / 12.0 * Math.PI) + 320.0 * Math.sin(y * Math.PI / 30.0)) * 2.0 / 3.0;
		return ret;
	}

	private static double transformLng(double x, double y) {
		double ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * Math.sqrt(Math.abs(x));
		ret += (20.0 * Math.sin(6.0 * x * Math.PI) + 20.0 * Math.sin(2.0 * x * Math.PI)) * 2.0 / 3.0;
		ret += (20.0 * Math.sin(x * Math.PI) + 40.0 * Math.sin(x / 3.0 * Math.PI)) * 2.0 / 3.0;
		ret += (150.0 * Math.sin(x / 12.0 * Math.PI) + 300.0 * Math.sin(x / 30.0 * Math.PI)) * 2.0 / 3.0;
		return ret;
	}

	public static boolean inPolygon(GeoPoint[] points, GeoPoint point) {
		int j = points.length - 1;
		boolean oddNodes = false;
		for (int i = 0; i < points.length; i++) {
			// 边界条件: 若点在多边形的顶点上
			if (point.equals(points[i])) {
				return true;
			}
			// 经纬度计算斜率(假定在一个平面内)
			double slop_A = (point.latitude - points[i].latitude) / (point.longitude - points[i].longitude);
			double slop_B = (point.latitude - points[j].latitude) / (point.longitude - points[j].longitude);
			if ((points[i].latitude < point.latitude && points[j].latitude >= point.latitude) || (points[j].latitude < point.latitude && points[i].latitude >= point.latitude)) {
				// 边界条件: 点在多边形边上,即与两个端点的斜率相等
				if (Math.abs(slop_A - slop_B) < 1e-6) {
					return true;
				}
				if (points[i].longitude + (point.latitude - points[i].latitude) / (points[j].latitude - points[i].latitude) * (points[j].longitude - points[i].longitude) < point.longitude) {
					oddNodes = !oddNodes;
				}
			}
			j = i;
		}
		return oddNodes;
	}

}
