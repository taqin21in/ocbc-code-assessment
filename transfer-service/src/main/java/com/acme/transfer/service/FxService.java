package com.acme.transfer.service;

import com.acme.transfer.client.FxClient;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Currency conversion, in {@link BigDecimal} end to end.
 *
 * <p>The vendor code computed the credit as {@code Math.round(amount.doubleValue() *
 * rate.doubleValue() * 100) / 100.0} and rounded the result {@code HALF_UP}, which is two bugs in
 * one: a {@code double} cannot hold the product of a large IDR amount and a rate with 8 decimals
 * exactly, and {@code HALF_UP} is not the rounding the contract specifies (INC-106). The credit is
 * now a single scaled {@code BigDecimal} multiplication with {@link RoundingMode#HALF_EVEN}, the
 * banker's rounding the API contract demands.
 *
 * <p>A rate past its {@code validUntil} instant is never used: the provider issues rates for 30
 * seconds, and honouring an expired one means quoting a price nobody offered.
 */
@Service
@RequiredArgsConstructor
public class FxService {

  private final FxClient fxClient;

  public Mono<Conversion> convert(BigDecimal amount, String fromCurrency, String toCurrency) {
    if (fromCurrency.equals(toCurrency)) {
      return Mono.just(new Conversion(amount, toCurrency, null));
    }
    return fxClient.getRate(fromCurrency, toCurrency)
        .handle((rate, sink) -> {
          Instant now = Instant.now();
          if (rate.validUntil() != null && rate.validUntil().isBefore(now)) {
            sink.error(new ExpiredRateException(fromCurrency, toCurrency, rate.validUntil(), now));
            return;
          }
          BigDecimal credit = amount.multiply(rate.rate())
              .setScale(2, RoundingMode.HALF_EVEN);
          sink.next(new Conversion(credit, toCurrency, rate.rate()));
        });
  }
}
