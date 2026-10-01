package com.acme.transfer.service;

/** One reconciliation pass, as the {@code POST /internal/reconciliation/run} contract returns it. */
public record ReconciliationStats(int checked, int completed, int rejected, int failed,
                                  int stillPending, long durationMs) {

  static ReconciliationStats empty(long durationMs) {
    return new ReconciliationStats(0, 0, 0, 0, 0, durationMs);
  }
}
