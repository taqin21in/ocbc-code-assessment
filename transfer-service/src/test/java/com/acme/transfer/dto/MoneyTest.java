package com.acme.transfer.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

/**
 * INC-106: money on the wire is always a two-place decimal, rounded with banker's rounding
 * (HALF_EVEN), the rounding the API contract specifies. The edge cases below are the ones a
 * double-based calculation gets wrong by 0.01.
 */
class MoneyTest {

  @Test
  void scalesToTwoPlacesHalfEven() {
    assertEquals("150.00", Money.of(new BigDecimal("150"), "USD").amount());
    assertEquals("150.00", Money.of(new BigDecimal("150.0"), "USD").amount());
    assertEquals("150.00", Money.of(new BigDecimal("150.000"), "USD").amount());
  }

  @Test
  void halfEvenRoundsToTheEvenNeighbour() {
    // 0.125 → 0.12 (2 is even), 0.135 → 0.14 (4 is even): the double path gives 0.12 and 0.14
    // inconsistently, or 0.13 for both, and the credit is off by a cent.
    assertEquals("0.12", Money.of(new BigDecimal("0.125"), "USD").amount());
    assertEquals("0.14", Money.of(new BigDecimal("0.135"), "USD").amount());
    assertEquals("0.13", Money.of(new BigDecimal("0.1251"), "USD").amount());
  }

  @Test
  void negativeAmountsRoundTheSameWay() {
    assertEquals("-0.12", Money.of(new BigDecimal("-0.125"), "USD").amount());
    assertEquals("-0.14", Money.of(new BigDecimal("-0.135"), "USD").amount());
  }

  @Test
  void carriesTheCurrency() {
    Money money = Money.of(new BigDecimal("99.999"), "IDR");
    assertEquals("100.00", money.amount());
    assertEquals("IDR", money.currency());
  }

  @Test
  void unknownAmountsStayNullRatherThanBecomingZero() {
    assertNull(Money.of(null, "USD"));
    assertNull(Money.of(new BigDecimal("99.99"), null));
  }
}
