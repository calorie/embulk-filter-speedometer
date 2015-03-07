package org.embulk.filter;

import org.embulk.filter.SpeedometerFilterPlugin.PluginTask;


class SpeedometerSpeedController {
    private final SpeedometerSpeedAggregator aggregator;

    private final long limitBytesPerSec;
    private final int maxSleepMillisec;
    private final int logIntervalMillisec;

    private long startTime;
    private volatile long periodStartTime;
    private volatile long periodTotalBytes;
    private volatile long threadTotalBytes;
    private volatile boolean renewFlag = true;

    SpeedometerSpeedController(PluginTask task, SpeedometerSpeedAggregator aggregator) {
        this.limitBytesPerSec = task.getSpeedLimit();
        this.maxSleepMillisec = task.getMaxSleepMillisec();
        this.logIntervalMillisec = task.getLogIntervalSeconds() * 1000;
        this.aggregator = aggregator;
    }

    public void stop() {
        startNewPeriod(0);
        aggregator.stopController(this);
    }

    public long getSpeedLimit() {
        return limitBytesPerSec;
    }

    public int getMaxSleepMillisec() {
        return maxSleepMillisec;
    }

    public int getLogIntervalMillisec() {
        return logIntervalMillisec;
    }

    public long getTotalBytes() {
        return threadTotalBytes + periodTotalBytes;
    }

    public long getPeriodBytesPerSec(long nowTime) {
        long timeDeltaMillisec = nowTime - periodStartTime;
        if (timeDeltaMillisec <= 0) {
            timeDeltaMillisec = 1;
        }
        return (periodTotalBytes * 1000) / timeDeltaMillisec;
    }

    public void checkSpeedLimit(long nowTime, long newDataSize) {
        if (startTime == 0) {
            startTime = nowTime;
            aggregator.startController(this, startTime);
        }

        if (renewFlag) {
            renewFlag = false;
            startNewPeriod(nowTime);
        }

        periodTotalBytes += newDataSize;
        aggregator.checkProgress(nowTime, logIntervalMillisec);

        if (limitBytesPerSec <= 0) {
            return;
        }

        long speedLimitForThread = aggregator.getSpeedLimitForController(this);
        long timeDeltaMillisec = nowTime > periodStartTime ? nowTime - periodStartTime : 1;
        long bytesPerSec = (periodTotalBytes * 1000) / timeDeltaMillisec;
        long overBytes = bytesPerSec - speedLimitForThread;

        if (overBytes > 0) {
            try {
                long sleepTime = (periodTotalBytes * 1000) / speedLimitForThread - timeDeltaMillisec;
                sleepTime = sleepTime > maxSleepMillisec ? maxSleepMillisec : sleepTime > 0 ? sleepTime : 0;
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                // TODO: Do I need to throw an exception ?
            }
        }
    }

    void renewPeriod() {
        renewFlag = true;
    }

    boolean isRenewPeriodSet() {
        return renewFlag;
    }

    SpeedometerSpeedAggregator getAggregator() {
        return aggregator;
    }

    private void startNewPeriod(long newPeriodTime) {
        threadTotalBytes += periodTotalBytes;
        periodTotalBytes = 0;
        periodStartTime = newPeriodTime;
    }
}

