package edu.touro.las.mcon364.final_test;

import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TelemetryProcessor – concurrent sensor-data pipeline
 *
 * Scenario: a fleet of devices continuously emits telemetry readings.
 * Each reading is represented as a {@link TelemetryEvent} carrying a device id,
 * a numeric metric value, and a nanosecond timestamp. Readings arrive faster than
 * they can be processed synchronously, so a multi-worker, queue-based pipeline
 * is required.
 *
 * Requirements:
 * - submit(event) enqueues an event so a worker thread can process it.
 *   It must throw {@link IllegalArgumentException} if event is null.
 *   Events submitted before start() is called must be silently discarded.
 * - start(workerCount) spins up {@code workerCount} worker threads that continuously
 *   drain the queue and process events. It must throw {@link IllegalArgumentException}
 *   if workerCount ≤ 0. Calling start() a second time must be a no-op(should make no difference).
 * - stop() signals all workers to finish, waits for them to terminate, then processes
 *   any events still left in the queue before returning.
 * - getTotalProcessed() returns the running total of events fully processed.
 * - getStats() returns a {@link DoubleSummaryStatistics} snapshot of all processed
 *   metric values. Each call must return a fresh, independent object.
 *
 * Thread-safety requirements:
 * - submit() and the read methods (getTotalProcessed, getStats) may be called
 *   concurrently from multiple threads without data loss or corruption.
 * - Use java.util.concurrent building blocks. Do not use raw synchronized blocks.
 */
public class TelemetryProcessor {

    // ── declare whatever fields you need ─────────────────────────────────────
    private final BlockingQueue<TelemetryEvent> queue = new LinkedBlockingQueue<>();

    // The worker threads, kept so stop() can wait for them to finish.
    private final List<Thread> workers = new ArrayList<>();

    // The shared on/off switch. volatile = every thread sees changes immediately.
    private volatile boolean running = false;

    // Thread-safe count of how many readings we've processed.
    private final AtomicInteger totalProcessed = new AtomicInteger();

    // Built-in accumulator: every time you call .accept(number) it updates the
    // count, min, max, sum, and average for you. It is NOT thread-safe on its own,
    // so below we wrap every use of it in `synchronized (stats)` (explained there).
    private final DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
    // ── public API ────────────────────────────────────────────────────────────

    /**
     * Add an event to the processing queue.
     *
     * Events submitted before {@link #start(int)} is called must be silently discarded.
     *
     * @param event the telemetry event to enqueue; must not be null
     * @throws IllegalArgumentException if event is null
     */
    public void submit(TelemetryEvent event) {
        //TODO - implement this method
        if (running){
            queue.offer(event);
        }
    }

    /**
     * Start processing events.
     * @param workerCount number of worker threads to create; must be ≥ 1
     * @throws IllegalArgumentException if workerCount ≤ 0
     */
    public void start(int workerCount) {
        //TODO - implement this method
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be positive");
        }
        running = true;   // turn on BEFORE launching so workers don't exit instantly
        for (int i = 0; i < workerCount; i++) {
            Thread worker = new Thread(this::workerLoop);
            workers.add(worker);
            worker.start();
        }
    }

    private void workerLoop() {
        // Keep working while running, OR while there is still leftover work to drain.
        while (running || !queue.isEmpty()) {
            try {
                // Wait up to 100ms for a reading; null means "nothing arrived, loop again".
                TelemetryEvent reading = queue.poll(100, TimeUnit.MILLISECONDS);
                if (reading != null) {
                    process(reading);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }


    private void process(TelemetryEvent reading) {
        totalProcessed.incrementAndGet();   // safe +1 across all workers

        // synchronized (stats) = "only one thread at a time may run this block."
        // DoubleSummaryStatistics.accept() is several internal steps; if two workers
        // ran it at once the numbers could come out wrong. The lock forces them to
        // take turns, keeping min/max/sum/count correct.
        synchronized (stats) {
            stats.accept(reading.metric());   // update count/min/max/sum/average with this value
        }
    }
    /**
     * Stop processing events.
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    public void stop() throws InterruptedException {
        //TODO - implement this method
        running = false;                 // tell workers to wind down
        for (Thread worker : workers) {
            worker.join();               // wait until each worker's loop fully ends
        }
        workers.clear();
    }

    /**
     * Return the total number of events that have been fully processed.
     */
    public int getTotalProcessed() {
        //TODO - implement this method
        return totalProcessed.get();
    }

    /**
     * Return a point-in-time snapshot of summary statistics for all processed
     * metric values (count, sum, min, max, average).
     *
     * Each call must return a <em>new</em>, independent {@link DoubleSummaryStatistics}
     * object so that callers cannot corrupt the internal state.
     *
     */
    public DoubleSummaryStatistics getStats() {
        DoubleSummaryStatistics snapshot = new DoubleSummaryStatistics();
        // Lock so we read a consistent picture while workers might be updating it,
        // then copy everything from the live stats into our fresh snapshot.
        synchronized (stats) {
            snapshot.combine(stats);     // combine = merge the live totals into the copy
        }
        return snapshot;                 // caller gets the copy; our internal stats stays private
    }
}
