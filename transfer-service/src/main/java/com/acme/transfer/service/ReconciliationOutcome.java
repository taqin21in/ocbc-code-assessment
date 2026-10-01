package com.acme.transfer.service;

/** The outcome of reconciliation for one transfer, counted in the pass result. */
public enum ReconciliationOutcome {
  COMPLETED, REJECTED, FAILED, STILL_PENDING
}
