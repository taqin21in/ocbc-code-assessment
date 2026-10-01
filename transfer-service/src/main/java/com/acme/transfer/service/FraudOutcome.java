package com.acme.transfer.service;

/** What the fraud screen decided for a transfer. */
public enum FraudOutcome {

  /** The transfer may go ahead. */
  ALLOW,

  /** The transfer must not happen. */
  DENY,

  /** The transfer waits for a human. */
  REVIEW,

  /**
   * The fraud service could not be reached. Not a decision to allow: no money may move, and the
   * transfer is held for review instead (see {@code FraudService}).
   */
  UNAVAILABLE
}
