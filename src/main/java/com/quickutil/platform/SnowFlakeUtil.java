package com.quickutil.platform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowFlakeUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(SnowFlakeUtil.class);
	private int machineId;
	private int serialId;

	//9223372036854 775 807 maxlong
	//1618403500105 123 110 时间戳-序列号-机器编号

	public SnowFlakeUtil(int machineId) {
		if (machineId < 0 || machineId > 1000) {
			machineId = -1;
			LOGGER.error("machine id should between 0 and 999");
		} else {
			this.machineId = machineId;
		}
	}

	public static void main(String[] args) {
		SnowFlakeUtil sf = new SnowFlakeUtil(10);
		for (int i = 0; i < 100000; i++) {
			System.out.println(sf.nextId());
		}
	}

	public Long nextId() {
		if (machineId == -1) {
			return null;
		}
		return System.currentTimeMillis() * 1000000 + getSerialId() * 1000 + machineId;
	}

	private synchronized int getSerialId() {
		serialId++;
		if (serialId >= 1000) {
			serialId = 0;
		}
		return serialId;
	}
}