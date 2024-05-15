package com.quickutil.platform;

import org.junit.Test;

public class FileUtilTest {

    @Test
    public void getHomePath() {
        System.out.println(FileUtil.getHomePath());
    }

    @Test
    public void sub() {
        System.out.println("(ENC)123".substring(5));
    }
}
