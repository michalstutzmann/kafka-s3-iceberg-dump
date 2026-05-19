package com.example.flinkiceberg;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;

import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory;

/**
 * A {@link TriggerLockFactory} decorator that retries {@link #open()} with a
 * fixed delay.
 *
 * <p>This exists to absorb the cold-connect race in
 * {@code JdbcLockFactory.open()} → {@code JdbcClientPool.newClient()}: on first
 * deploy the maintenance operators' very first JDBC connection to Postgres can
 * fail transiently (it succeeds within seconds). {@code open()} runs on the
 * <em>TaskManager</em> operator ({@code LockRemover.open}), so a client-side
 * pre-flight in {@code main()} cannot prevent it — the retry has to happen
 * here, in-place, otherwise the whole job restarts. Wrapping only {@code open()}
 * is sufficient: the per-cycle lock create/release calls run after the pool is
 * established and were never observed to fail.
 *
 * <p>{@link java.io.Serializable} via the interface — Flink ships this to the
 * TaskManager, so the delegate (a {@code JdbcLockFactory}) and the int/long
 * fields must stay serializable.
 */
final class RetryingTriggerLockFactory implements TriggerLockFactory {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = System.getLogger(RetryingTriggerLockFactory.class.getName());

  private final TriggerLockFactory delegate;
  private final int attempts;
  private final long delayMillis;

  RetryingTriggerLockFactory(TriggerLockFactory delegate, int attempts, long delayMillis) {
    this.delegate = delegate;
    this.attempts = attempts;
    this.delayMillis = delayMillis;
  }

  @Override
  public void open() {
    RuntimeException last = null;
    for (int attempt = 1; attempt <= attempts; attempt++) {
      try {
        delegate.open();
        if (attempt > 1) {
          LOG.log(
              Level.INFO,
              "TriggerLockFactory.open() succeeded on attempt {0}/{1}",
              attempt,
              attempts);
        }
        return;
      } catch (RuntimeException e) {
        last = e;
        LOG.log(
            Level.WARNING,
            "TriggerLockFactory.open() failed (attempt {0}/{1}): {2} — retrying in {3} ms",
            attempt,
            attempts,
            e.getMessage(),
            delayMillis);
        try {
          Thread.sleep(delayMillis);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Interrupted while retrying lock factory open()", ie);
        }
      }
    }
    throw last;
  }

  @Override
  public Lock createLock() {
    return delegate.createLock();
  }

  @Override
  public Lock createRecoveryLock() {
    return delegate.createRecoveryLock();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
