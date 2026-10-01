package com.acme.transfer.web;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.server.WebExceptionHandler;

/** Registers the problem detail handler above every other error renderer. */
@Configuration
public class ProblemWebExceptionHandlerConfiguration {

  @Bean
  public WebExceptionHandler problemWebExceptionHandler() {
    return new ProblemWebExceptionHandler();
  }
}
