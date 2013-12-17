package com.jordanwilliams.heftydb.metrics;

import net.jcip.annotations.ThreadSafe;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

@ThreadSafe
public class HistogramMetric implements Metric {

    private static final int SAMPLE_COUNT = 10000;

    private final String name;
    private final String units;

    private double runningCount = 0;
    private double[] samples;
    private final AtomicInteger samplePos = new AtomicInteger();
    private final AtomicInteger sampleCount = new AtomicInteger();


    public HistogramMetric(String name, String units, int sampleCount) {
        this.name = name;
        this.units = units;
        this.samples = new double[sampleCount];
    }

    public HistogramMetric(String name, String units) {
        this(name, units, SAMPLE_COUNT);
    }

    public HistogramMetric(String name) {
        this(name, "", SAMPLE_COUNT);
    }

    public synchronized void record(double sample) {
        if (samplePos.get() >= SAMPLE_COUNT) {
            samplePos.set(0);
        }

        samples[samplePos.get()] = sample;
        samplePos.incrementAndGet();
        sampleCount.incrementAndGet();
        runningCount += sample;
    }

    public synchronized double percentile(double percentile) {
        double[] sampleArray = sampleArray();
        int percentileSample = (int) Math.floor((double) sampleArray.length * (percentile / 100));
        Arrays.sort(sampleArray);
        return samplePos.get() == 0 ? 0 : sampleArray[percentileSample];
    }

    public synchronized double avg() {
        return runningCount / sampleCount.get();
    }

    private double[] sampleArray() {
        if (sampleCount.get() < SAMPLE_COUNT) {
            return Arrays.copyOfRange(samples, 0, samplePos.get());
        }

        return samples;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized String summary() {
        if (samples.length == 0) {
            return "";
        }

        StringBuilder str = new StringBuilder();
        str.append(name + " ");

        str.append("p10:" + percentile(10) + units + ", ");
        str.append("p50:" + percentile(50) + units + ", ");
        str.append("p90:" + percentile(90) + units + ", ");
        str.append("p99:" + percentile(99) + units + ", ");
        str.append("avg:" + avg() + units);

        return str.toString();
    }
}
