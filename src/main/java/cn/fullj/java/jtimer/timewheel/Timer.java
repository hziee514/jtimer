package cn.fullj.java.jtimer.timewheel;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author bruce.wu
 * @since 2019/6/14 15:15
 */
public interface Timer {

    Job newJob(TimerTask task, long delay, TimeUnit unit);

    Set<Job> stop();

}
