package com.acme.transfer.events;

import java.time.Instant;

/**
 * One status change of one transfer, published to the live feed.
 *
 * @param sourceAccount kept for server-side filtering, never sent to subscribers: the SSE payload
 *                       is the four fields {@code docs/api.md} specifies
 */
public record TransferEvent(String transferId, String sourceAccount, String status,
                            String reasonCode, Instant updatedAt, int sequence) {
}
