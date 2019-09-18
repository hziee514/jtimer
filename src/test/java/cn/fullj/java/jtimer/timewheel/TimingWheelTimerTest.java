package cn.fullj.java.jtimer.timewheel;

import org.junit.Test;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author bruce.wu
 * @since 2019/9/18 13:26
 */
public class TimingWheelTimerTest {

    @Test
    public void test_exec() throws Exception {
        final int count = 10;
        final CountDownLatch latch = new CountDownLatch(5);
        final TimingWheelTimer timer = new TimingWheelTimer(10, TimeUnit.MILLISECONDS);
        System.out.println(" start at " + System.currentTimeMillis());
        Job job1 = null;
        for (int i = 0; i < count; i++) {
            final String idx = Integer.toHexString(i);
            job1 = timer.newJob((timer1, job) -> {
                System.out.println("exec " + idx + " at " + System.currentTimeMillis());
                latch.countDown();
            }, i * 100, TimeUnit.MILLISECONDS);
        }
        if (job1 != null) {
            job1.cancel();
        }
        latch.await();
        Set<Job> jobs = timer.stop();
        System.out.println(jobs.size());
    }

}