package com.quickutil.platform.def;

public class GeoPoint {
	public double latitude = 0.0;
	public double longitude = 0.0;

	public GeoPoint(double lat, double lng) {
		this.latitude = lat;
		this.longitude = lng;
	}

	public boolean equals(GeoPoint o) {
		if (this.longitude == o.longitude && this.latitude == o.latitude)
			return true;
		return false;
	}
}