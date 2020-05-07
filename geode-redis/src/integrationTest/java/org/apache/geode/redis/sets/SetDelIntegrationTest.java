/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.redis.sets;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.internal.cli.util.ThreePhraseGenerator;
import org.apache.geode.redis.GeodeRedisServer;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class SetDelIntegrationTest {
  static Jedis jedis;
  static Jedis jedis2;
  private static GeodeRedisServer server;
  private static GemFireCache cache;
  private static ThreePhraseGenerator generator = new ThreePhraseGenerator();
  private static int port = 6379;

  @BeforeClass
  public static void setUp() {
    CacheFactory cf = new CacheFactory();
    cf.set(LOG_LEVEL, "error");
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    cache = cf.create();
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    server = new GeodeRedisServer("localhost", port);

    server.start();
    jedis = new Jedis("localhost", port, 10000000);
    jedis2 = new Jedis("localhost", port, 10000000);
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    jedis2.close();
    cache.close();
    server.shutdown();
  }

  @After
  public void cleanup() {
    jedis.flushAll();
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testDelSet() {
    String source = generator.generate('x');
    int elements = 10;
    String[] strings = generateStrings(elements, 'y');
    jedis.sadd(source, strings);

    Long result = jedis.del(source);

    assertThat(result).isEqualTo(1L);
    assertThat(jedis.exists(source)).isFalse();
    assertThat(jedis.sismember(source, strings[1])).isFalse();
  }

  @Test
  public void testDelNegativeCase() {
    Long result = jedis.del("nonexistentKey");
    assertThat(result).isEqualTo(0L);
  }

  @Test
  public void testConcurrentDelSet() throws ExecutionException, InterruptedException {
    int setCount = 1000;
    String[] setOfSets1 = generateStrings(setCount, 'x');
    String[] setOfSets2 = generateStrings(setCount, 'y');

    for (String entry : setOfSets1) {
      jedis.sadd(entry, "singleValue");
    }
    for (String entry : setOfSets2) {
      jedis.sadd(entry, "singleValue");
    }

    ExecutorService pool = Executors.newFixedThreadPool(2);
    Callable<Long> callable1 = () -> deleteSet(setOfSets1, jedis);
    Callable<Long> callable2 = () -> deleteSet(setOfSets2, jedis2);
    Future<Long> future1 = pool.submit(callable1);
    Future<Long> future2 = pool.submit(callable2);

    assertThat(future1.get() + future2.get()).isEqualTo(new Long(setOfSets1.length)
        + new Long(setOfSets2.length));
    for (String entry : setOfSets1) {
      assertThat(jedis.exists(entry)).isFalse();
    }
    for (String entry : setOfSets2) {
      assertThat(jedis.exists(entry)).isFalse();
    }
  }

  private long deleteSet(String[] setOfSets,
                         Jedis jedis) {
    long results = 0;
    for (String entry : setOfSets) {
      results += jedis.del(entry);
      Thread.yield();
    }
    return results;
  }

  private String[] generateStrings(int elements, char uniqueElement) {
    Set<String> strings = new HashSet<>();
    for (int i = 0; i < elements; i++) {
      String elem = generator.generate(uniqueElement);
      strings.add(elem);
    }
    return strings.toArray(new String[strings.size()]);
  }
}
