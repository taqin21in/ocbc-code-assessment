package com.acme.transfer.web;

/**
 * An RFC 9457 problem detail. Every error response of the API has this shape.
 *
 * @param type the problem type, {@code about:blank} for all problems this service produces
 * @param title a short human readable summary of the problem
 * @param status the HTTP status generated for this occurrence
 * @param detail a specific explanation, safe to show to customers and support
 * @param instance the path of the request that hit the problem
 * @param code the machine readable error code
 * @param correlationId the correlation ID of the request, so support finds it in the logs
 */
public record Problem(String type, String title, int status, String detail, String instance,
                      String code, String correlationId) {

  public static final String MEDIA_TYPE = "application/problem+json";
}
