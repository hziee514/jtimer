package cn.fullj.java.jtimer.timewheel;

/**
 * @author bruce.wu
 * @since 2019/6/14 15:17
 */
@FunctionalInterface
public interface TimerTask {

    void run(Timer timer, Job job) throws Exception;

}
