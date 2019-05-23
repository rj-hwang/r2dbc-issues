package com.rjhwang.test.r2dbc;

import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * <a href="https://github.com/r2dbc/r2dbc-h2/issues/77">r2dbc-h2/issues#77 - Should not decode null integer value to 0</a>
 *
 * @author RJ
 */
@TestInstance(PER_CLASS)
class R2dbcH2Issue77Test {
  private ConnectionFactory connectionFactory = new H2ConnectionFactory(new H2ConnectionConfiguration.Builder()
    .inMemory("testdb")
    .username("tester")
    .password("password")
    .build());

  private String createTableSql = "create table t(id integer primary key, v integer)";
  private String insertSql = "insert into t(id, v) values (1, null), (2, 2)";

  @BeforeAll
  void init() {
    StepVerifier.create(
      Mono.from(connectionFactory.create())
        .flatMap(c -> Mono.from(c.createStatement(createTableSql).execute()).then(Mono.just(c)))
        .flatMap(c -> Mono.from(c.createStatement(insertSql).execute()).then())
    ).verifyComplete();
  }

  @Test
  void testNullValue() {
    StepVerifier.create(
      Mono.from(connectionFactory.create())
        .flatMap(c -> Mono.from(c.createStatement("select * from t where id = 1").execute()))
        .flatMap(result -> Mono.from(result.map((row, rowMetadata) -> row.get("v", Integer.class))))
    ).expectNext((Integer) null).verifyComplete();
  }

  @Test
  void testNotNullValue() {
    StepVerifier.create(
      Mono.from(connectionFactory.create())
        .flatMap(c -> Mono.from(c.createStatement("select * from t where id = 2").execute()))
        .flatMap(result -> Mono.from(result.map((row, rowMetadata) -> row.get("v", Integer.class))))
    ).expectNext(2).verifyComplete();
  }
}