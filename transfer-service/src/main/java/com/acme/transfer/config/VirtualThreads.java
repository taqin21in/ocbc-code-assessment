package com.acme.transfer.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The blocking boundary.
 *
 * <p>One executor that starts a fresh virtual thread per task. All blocking work in the service —
 * the core banking SDK, and the reconciliation worker — runs through it. The WebFlux event loop
 * never touches it. Virtual threads are cheap, so nothing here is a pool and nothing is sized by
 * expected load: concurrency is limited where the expensive resource is, in
 * {@code DefaultCoreGateway}'s semaphore.
 */
@Configuration
public class VirtualThreads {

  @Bean(name = "virtualThreadExecutor", destroyMethod = "shutdown")
  public ExecutorService virtualThreadExecutor() {
    return Executors.newVirtualThreadPerTaskExecutor();
  }
}
