package cn.fullj.java.jtimer.timewheel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author bruce.wu
 * @since 2019/6/14 15:15
 */
public class TimingWheelTimer implements Timer {

    private static final Logger logger = LoggerFactory.getLogger(TimingWheelTimer.class);

    private static final int WORKER_INIT = 0;
    private static final int WORKER_STARTED = 1;
    private static final int WORKER_SHUTDOWN = 2;

    private final Worker worker = new Worker();
    private final AtomicInteger workerState = new AtomicInteger(WORKER_INIT);
    private final CountDownLatch workerInitialized = new CountDownLatch(1);
    private final Thread workThread;

    private final long tickDuration;
    private final TimingWheelBucket[] wheel;
    private final int mask;

    private final Queue<TimingWheelJob> jobs = new LinkedBlockingQueue<>();
    private final Queue<TimingWheelJob> cancelledJobs = new LinkedBlockingQueue<>();

    private volatile long startTime;

    public TimingWheelTimer() {
        this(Executors.defaultThreadFactory());
    }

    public TimingWheelTimer(long tickDuration, TimeUnit unit) {
        this(Executors.defaultThreadFactory(), tickDuration, unit);
    }

    public TimingWheelTimer(long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this(Executors.defaultThreadFactory(), tickDuration, unit, ticksPerWheel);
    }

    public TimingWheelTimer(ThreadFactory threadFactory) {
        this(threadFactory, 100, TimeUnit.MILLISECONDS);
    }

    public TimingWheelTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit) {
        this(threadFactory, tickDuration, unit, 512);
    }

    public TimingWheelTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit, int ticksPerWheel) {
        Objects.requireNonNull(threadFactory, "threadFactory");
        Objects.requireNonNull(unit, "unit");
        if (tickDuration <= 0) {
            throw new IllegalArgumentException("tickDuration must be greater than 0: " + tickDuration);
        }
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException("ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }
        this.wheel = createWheel(ticksPerWheel);
        this.mask = wheel.length - 1;
        this.tickDuration = unit.toNanos(tickDuration);
        this.workThread = threadFactory.newThread(worker);
    }

    private static TimingWheelBucket[] createWheel(int ticksPerWheel) {
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException("ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }
        if (ticksPerWheel > 0x10000000) {
            throw new IllegalArgumentException("ticksPerWheel must be less than 0x10000000: " + ticksPerWheel);
        }
        int normalizedTicksPerWheel = 1;
        while (normalizedTicksPerWheel < ticksPerWheel) {
            normalizedTicksPerWheel <<= 1;
        }
        TimingWheelBucket[] wheel = new TimingWheelBucket[normalizedTicksPerWheel];
        for (int i = 0; i < wheel.length; i++) {
            wheel[i] = new TimingWheelBucket();
        }
        return wheel;
    }

    /**
     * start worker thread
     */
    private void start() {
        switch (workerState.get()) {
            case WORKER_INIT:
                if (workerState.compareAndSet(WORKER_INIT, WORKER_STARTED)) {
                    workThread.start();
                }
                break;
            case WORKER_STARTED:
                break;
            case WORKER_SHUTDOWN:
                throw new IllegalStateException("Cannot start once stopped");
            default:
                throw new Error("Invalid worker state");
        }
        while (startTime == 0) {
            try {
                workerInitialized.await();
            } catch (InterruptedException ignored) {

            }
        }
    }

    @Override
    public Job newJob(TimerTask task, long delay, TimeUnit unit) {
        Objects.requireNonNull(task, "task");
        Objects.requireNonNull(unit, "unit");

        //TODO check queue size

        start();

        long deadline = System.nanoTime() + unit.toNanos(delay) - startTime;
        if (delay > 0 && deadline < 0) {
            deadline = Long.MAX_VALUE;
        }
        TimingWheelJob job = new TimingWheelJob(this, task, deadline);
        jobs.offer(job);
        return job;
    }

    @Override
    public Set<Job> stop() {
        if (Thread.currentThread() == workThread) {
            throw new IllegalStateException(TimingWheelTimer.class.getSimpleName()
                    + ".stop() can not be called from " + TimerTask.class.getSimpleName());
        }
        if (!workerState.compareAndSet(WORKER_STARTED, WORKER_SHUTDOWN)) {
            if (workerState.getAndSet(WORKER_SHUTDOWN) != WORKER_SHUTDOWN) {
                //TODO
            }
            return Collections.emptySet();
        }
        while (workThread.isAlive()) {
            workThread.interrupt();
            try {
                workThread.join(100);
            } catch (InterruptedException ignored) {

            }
        }
        return worker.getUnprocessedJobs();
    }

    private final class Worker implements Runnable {

        private final Set<Job> unprocessedJobs = new HashSet<>();

        private long tick;

        @Override
        public void run() {
            Thread.currentThread().setName(TimingWheelTimer.class.getSimpleName());

            startTime = System.nanoTime();
            if (startTime == 0) {
                startTime = 1;
            }
            workerInitialized.countDown();

            do {
                final long deadline = waitForNextTick();
                if (deadline > 0) {
                    removeCancelledJobs();
                    transferJobsToBucket();

                    int idx = (int)(tick & mask);
                    wheel[idx].expire(deadline);

                    tick++;
                }
            } while (workerState.get() == WORKER_STARTED);

            for (TimingWheelBucket bucket : wheel) {
                bucket.clear(unprocessedJobs);
            }
            for (;;) {
                TimingWheelJob job = jobs.poll();
                if (job == null) {
                    break;
                }
                if (!job.isCancelled()) {
                    unprocessedJobs.add(job);
                }
            }
            removeCancelledJobs();
        }

        private void transferJobsToBucket() {
            for (int i = 0; i < 10000; i++) {
                TimingWheelJob job = jobs.poll();
                if (job == null) {
                    break;
                }
                if (job.state() == TimingWheelJob.STATE_CANCELLED) {
                    continue;
                }
                long totalTick = job.deadline / tickDuration;
                job.remainingRounds = (totalTick - tick) / wheel.length;

                totalTick = Math.max(totalTick, tick);

                int idx = (int) (totalTick & mask);
                wheel[idx].append(job);
            }
        }

        private void removeCancelledJobs() {
            for (;;) {
                TimingWheelJob job = cancelledJobs.poll();
                if (job == null) {
                    break;
                }
                job.remove();
            }
        }

        private long waitForNextTick() {
            long deadline = tickDuration * (tick + 1);
            for (;;) {
                final long currentTime = System.nanoTime() - startTime;
                long sleepMs = (deadline - currentTime + 999999) / 1000000;
                if (sleepMs <= 0) {
                    return  currentTime == Long.MIN_VALUE ? -Long.MAX_VALUE : currentTime;
                }
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ignored) {
                    if (workerState.get() == WORKER_SHUTDOWN) {
                        return Long.MIN_VALUE;
                    }
                }
            }
        }

        Set<Job> getUnprocessedJobs() {
            return Collections.unmodifiableSet(unprocessedJobs);
        }

    }

    private static final class TimingWheelBucket {

        private TimingWheelJob head;
        private TimingWheelJob tail;

        void append(TimingWheelJob job) {
            assert job.bucket == null;
            job.bucket = this;
            if (head == null) {
                head = tail = job;
            } else {
                tail.next = job;
                job.prev = tail;
                tail = job;
            }
        }

        /**
         * expire all TimingWheelJob for given deadline
         */
        void expire(long deadline) {
            TimingWheelJob job = head;
            while (job != null) {
                TimingWheelJob next = job.next;
                if (job.remainingRounds <= 0) {
                    next = remove(job);
                    if (job.deadline <= deadline) {
                        job.expire();
                    } else {
                        throw new IllegalStateException(String.format(
                                "job.deadline (%d) > deadline (%d)", job.deadline, deadline));
                    }
                } else if (job.isCancelled()) {
                    next = remove(job);
                } else {
                    job.remainingRounds--;
                }
                job = next;
            }
        }

        TimingWheelJob remove(TimingWheelJob job) {
            TimingWheelJob next = job.next;
            if (job.prev != null) {
                job.prev.next = next;
            }
            if (job.next != null) {
                job.next.prev = job.prev;
            }

            if (job == head) {
                if (job == tail) {
                    head = tail = null;
                } else {
                    head = next;
                }
            } else if (job == tail) {
                tail = job.prev;
            }

            job.prev = null;
            job.next = null;
            job.bucket = null;
            return next;
        }

        void clear(Set<Job> set) {
            for (;;) {
                TimingWheelJob top = poll();
                if (top == null) {
                    break;
                }
                if (top.isExpired() || top.isCancelled()) {
                    continue;
                }
                set.add(top);
            }
        }

        private TimingWheelJob poll() {
            TimingWheelJob head = this.head;
            if (head == null) {
                return null;
            }
            TimingWheelJob next = head.next;
            if (next == null) {
                this.tail = this.head = null;
            } else {
                this.head = next;
                next.prev = null;
            }
            head.prev = null;
            head.next = null;
            head.bucket = null;
            return head;
        }

    }

    private static final class TimingWheelJob implements Job {

        private static final int STATE_INIT = 0;
        private static final int STATE_CANCELLED = 1;
        private static final int STATE_EXPIRED = 2;

        private final TimingWheelTimer timer;
        private final TimerTask task;
        private final long deadline;

        private final AtomicInteger state = new AtomicInteger(STATE_INIT);

        long remainingRounds;

        // double link list point
        TimingWheelJob next;
        TimingWheelJob prev;

        TimingWheelBucket bucket;

        TimingWheelJob(TimingWheelTimer timer, TimerTask task, long deadline) {
            this.timer = timer;
            this.task = task;
            this.deadline = deadline;
        }

        int state() {
            return state.get();
        }

        @Override
        public Timer timer() {
            return this.timer;
        }

        @Override
        public TimerTask task() {
            return this.task;
        }

        @Override
        public boolean isExpired() {
            return state.get() == STATE_EXPIRED;
        }

        @Override
        public boolean isCancelled() {
            return state.get() == STATE_CANCELLED;
        }

        @Override
        public boolean cancel() {
            if (!state.compareAndSet(STATE_INIT, STATE_CANCELLED)) {
                return false;
            }
            timer.cancelledJobs.offer(this);
            return true;
        }

        void expire() {
            if (!state.compareAndSet(STATE_INIT, STATE_EXPIRED)) {
                return;
            }
            try {
                task.run(timer, this);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("An exception was thrown by {}", task.getClass().getSimpleName(), t);
                }
            }
        }

        void remove() {
            TimingWheelBucket bucket = this.bucket;
            if (bucket != null) {
                bucket.remove(this);
            }
        }

        @Override
        public String toString() {
            final long currentTime = System.nanoTime();
            long remaining = deadline - currentTime + timer.startTime;
            StringBuilder sb = new StringBuilder(128)
                    .append(getClass().getName())
                    .append('(')
                    .append("deadline: ").append(remaining).append(" ns");
            if (isCancelled()) {
                sb.append(", cancelled");
            }
            return sb.append(", task: ")
                    .append(task())
                    .append(')')
                    .toString();
        }
    }

}
