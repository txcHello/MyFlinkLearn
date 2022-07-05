package bigdata.app;

import bigdata.bean.Event;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author Administrator
 * @Date 2022/7/5 12:52
 * @Version 1.0
 * Desc:
 */
public class TestDemo {
    public static void main(String[] args) {
        System.out.println(getWindowStartWithOffset(1656997370000l, 0l, 6000));
    }

    public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
        final long remainder = (timestamp - offset) % windowSize;
        // handle both positive and negative cases
        if (remainder < 0) {
            return timestamp - (remainder + windowSize);
        } else {
            return timestamp - remainder;
        }
    }
}
