package com.quickutil.platform.entity;

import com.quickutil.platform.JsonUtil;

public class GeoDef {
	public double latitude;
	public double longitude;
	public String countryCode;
	public String country;
	public String countryChinese;
	public String stateCode;
	public String state;
	public String stateChinese;
	public String city;
	public String description;

	public GeoDef(double latitude, double longitude, String countryCode, String country, String countryChinese, String stateCode, String state, String stateChinese, String city, String description) {
		this.latitude = latitude;
		this.longitude = longitude;
		this.countryCode = countryCode;
		this.country = country;
		this.countryChinese = countryChinese;
		this.stateCode = stateCode;
		this.state = state;
		this.stateChinese = stateChinese;
		this.city = city;
		this.description = description;
	}

	public String toString() {
		return JsonUtil.toJson(this);
	}
}
