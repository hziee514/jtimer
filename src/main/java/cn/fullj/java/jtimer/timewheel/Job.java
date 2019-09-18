package cn.fullj.java.jtimer.timewheel;

/**
 * Execution plan in timer
 *
 * @author bruce.wu
 * @since 2019/6/14 15:17
 */
public interface Job {

    Timer timer();

    TimerTask task();

    boolean isExpired();

    boolean isCancelled();

    boolean cancel();

}
