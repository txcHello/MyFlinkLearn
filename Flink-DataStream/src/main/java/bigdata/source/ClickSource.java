package bigdata.source;


import bigdata.bean.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    private boolean running = true;
    private String[] userArr = {"mary", "bob", "alice", "liz"};
    private String[] urlArr = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
    private Random random = new Random();
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        while (running) {
            // collect方法，向下游发送数据
            ctx.collect(
                    new Event(
                            userArr[random.nextInt(userArr.length)],
                            urlArr[random.nextInt(urlArr.length)],
                            Calendar.getInstance().getTimeInMillis()
                    )
            );
            Thread.sleep(100L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
