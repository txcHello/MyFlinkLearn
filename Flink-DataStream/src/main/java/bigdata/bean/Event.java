package bigdata.bean;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author Administrator
 * @Date 2022/7/4 16:22
 * @Version 1.0
 * Desc:
 */
public class Event {
     public  String user;
     public  String  url;
     public  Long  timestamp;

    public Event(String user, String url, Long timestamp) {

        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {


        SimpleDateFormat sdf  = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + timestamp + " 日期=" + sdf.format(new Date(timestamp))
                +
                '}';
    }
}
