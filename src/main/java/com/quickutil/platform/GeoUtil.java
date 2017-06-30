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
import java.util.List;

import org.apache.http.HttpResponse;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.quickutil.platform.def.GeoDef;

public class GeoUtil {

	private static DatabaseReader databaseReader = null;

	static {
		init();
	}

	private static void init() {
		try {
			String mmdbFile = FileUtil.getCurrentPath() + "/GeoIP2-City.mmdb";
			File database = new File(mmdbFile);
			if (!database.exists()) {
				HttpResponse response = HttpUtil.httpGet("http://quickutil.oss-cn-shenzhen.aliyuncs.com/GeoIP2-City.mmdb");
				byte[] mmdb = FileUtil.stream2byte(response.getEntity().getContent());
				if (mmdb != null)
					FileUtil.byte2File(mmdbFile, mmdb);
				database = new File(mmdbFile);
			}
			databaseReader = new DatabaseReader.Builder(database).build();
			System.out.println("GeoUtil loaded successfully");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 根据IP查询地理信息
	 * 
	 * @param ip-ip地址
	 * @return
	 */
	public static GeoDef GeoIPByMMDB(String ip) {
		try {
			InetAddress ipAddr = InetAddress.getByName(ip);
			CityResponse result = databaseReader.city(ipAddr);
			String countryCode = result.getCountry().getIsoCode();
			String country = result.getCountry().getName();
			String stateCode = result.getMostSpecificSubdivision().getIsoCode();
			String state = result.getMostSpecificSubdivision().getName();
			String city = result.getCity().getName();
			Double latitude = result.getLocation().getLatitude();
			Double longitude = result.getLocation().getLongitude();
			if (latitude == null)
				latitude = 0.0;
			if (longitude == null)
				longitude = 0.0;
			return new GeoDef(latitude, longitude, countryCode, country, stateCode, state, city, "");
		} catch (UnknownHostException e) {
		} catch (GeoIp2Exception e) {
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
			return new GeoDef(latitude, longitude, "", country, "", province, city, description);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
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
			String province = object.getAsJsonObject("result").getAsJsonObject("addressComponent").get("province").getAsString();
			String city = object.getAsJsonObject("result").getAsJsonObject("addressComponent").get("city").getAsString();
			String description = object.getAsJsonObject("result").getAsJsonObject("addressComponent").get("district").getAsString();
			return new GeoDef(latitude, longitude, "", country, "", province, city, description);
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
	public static List<GeoDef> geoCodeyByBaidu(List<Double> points) {
		try {
			if (points.size() > 40)
				return null;
			StringBuilder builder = new StringBuilder();
			for (int i = 0; i < points.size(); i = i + 2) {
				double[] delta = WGSToGCJPointer(points.get(i), points.get(i + 1));
				delta = GCJToBDPointer(delta[0], delta[1]);
				builder.append(delta[0] + "," + delta[1] + "|");
			}
			String apiKey = "RMVaRr172UQuTGVVTDKz1eGD";
			String queryUrl = String.format("http://api.map.baidu.com/geocoder/v2/?ak=%s&location=%s&output=json&batch=true", apiKey,
					URLEncoder.encode(builder.substring(0, builder.length() - 1), "UTF-8"));
			JsonObject object = JsonUtil.toJsonMap(FileUtil.stream2string(HttpUtil.httpGet(queryUrl).getEntity().getContent()));
			JsonArray array = object.getAsJsonArray("areas");
			List<GeoDef> geodefList = new ArrayList<GeoDef>();
			for (int i = 0; i < points.size(); i = i + 2) {
				geodefList.add(new GeoDef(points.get(i), points.get(i + 1), "", array.get(i / 2).getAsJsonObject().get("country").getAsString(), "",
						array.get(i / 2).getAsJsonObject().get("province").getAsString(), array.get(i / 2).getAsJsonObject().get("city").getAsString(),
						array.get(i / 2).getAsJsonObject().get("district").getAsString()));
			}
			return geodefList;
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

}
