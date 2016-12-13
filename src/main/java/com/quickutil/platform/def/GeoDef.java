package com.quickutil.platform.def;

import com.quickutil.platform.JsonUtil;

public class GeoDef {
    public double latitude;
    public double longitude;
    public String countryCode;
    public String country;
    public String stateCode;
    public String state;
    public String city;
    public String description;

    public GeoDef(double latitude, double longitude, String countryCode, String country, String stateCode, String state, String city, String description) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.countryCode = countryCode;
        this.country = country;
        this.stateCode = stateCode;
        this.state = state;
        this.city = city;
        this.description = description;
    }

    public String toString() {
        return JsonUtil.toJson(this);
    }
}
