/**
 * Redis-semantics priority/visibility lease queue (`ZSET` + atomic lock token).
 * Handles delayed schedules, concurrency keys, lease heartbeats, and crash reclamation.
 */

export interface QueuedLease {
  runId: string;
  attemptId: string;
  attemptNumber: number;
  concurrencyKey: string;
  leaseToken: string;
  availableAtMs: number;
  leaseExpiresAtMs: number | null;
}

export class RedisLeaseBroker {
  private entries = new Map<string, QueuedLease>();
  private inFlightByKey = new Map<string, number>();

  /** Enqueues or reschedules an attempt with atomic score timestamp (ms). */
  enqueue(entry: Omit<QueuedLease, "leaseToken" | "leaseExpiresAtMs">): void {
    this.entries.set(entry.attemptId, {
      ...entry,
      leaseToken: "",
      leaseExpiresAtMs: null,
    });
  }

  /**
   * Simulates Redis Lua `ZPOPMIN`-style lease acquisition honoring per-key concurrency limits
   * and reclaiming expired leases if a worker crashed before acking.
   */
  claimNext(
    nowMs: number,
    leaseDurationMs: number,
    maxPerConcurrencyKey: number,
    newLeaseToken: string,
  ): QueuedLease | null {
    // First reclaim expired in-flight leases
    for (const item of this.entries.values()) {
      if (item.leaseExpiresAtMs !== null && item.leaseExpiresAtMs <= nowMs) {
        const cur = this.inFlightByKey.get(item.concurrencyKey) ?? 0;
        if (cur > 0) this.inFlightByKey.set(item.concurrencyKey, cur - 1);
        item.leaseExpiresAtMs = null;
        item.leaseToken = "";
        item.availableAtMs = nowMs;
      }
    }

    const candidates = Array.from(this.entries.values())
      .filter((item) => item.leaseExpiresAtMs === null && item.availableAtMs <= nowMs)
      .sort((a, b) => a.availableAtMs - b.availableAtMs);

    for (const candidate of candidates) {
      const active = this.inFlightByKey.get(candidate.concurrencyKey) ?? 0;
      if (active >= maxPerConcurrencyKey) continue;
      candidate.leaseToken = newLeaseToken;
      candidate.leaseExpiresAtMs = nowMs + leaseDurationMs;
      this.inFlightByKey.set(candidate.concurrencyKey, active + 1);
      return { ...candidate };
    }
    return null;
  }

  /** Extends lease TTL only if fencing token matches current owner (prevents split-brain ack). */
  heartbeat(attemptId: string, leaseToken: string, nowMs: number, extensionMs: number): boolean {
    const item = this.entries.get(attemptId);
    if (!item || item.leaseToken !== leaseToken || item.leaseExpiresAtMs === null) {
      return false;
    }
    if (item.leaseExpiresAtMs <= nowMs) return false;
    item.leaseExpiresAtMs = nowMs + extensionMs;
    return true;
  }

  /** Acknowledges completion and removes entry using fencing token check. */
  ack(attemptId: string, leaseToken: string): boolean {
    const item = this.entries.get(attemptId);
    if (!item || item.leaseToken !== leaseToken) return false;
    this.entries.delete(attemptId);
    const active = this.inFlightByKey.get(item.concurrencyKey) ?? 0;
    if (active > 0) this.inFlightByKey.set(item.concurrencyKey, active - 1);
    return true;
  }

  /** Removes an attempt from queue tracking (e.g., suspended at waitpoint or dead-lettered). */
  release(attemptId: string, leaseToken: string): boolean {
    return this.ack(attemptId, leaseToken);
  }

  depth(): number {
    return this.entries.size;
  }

  inFlightCount(concurrencyKey: string): number {
    return this.inFlightByKey.get(concurrencyKey) ?? 0;
  }
}
