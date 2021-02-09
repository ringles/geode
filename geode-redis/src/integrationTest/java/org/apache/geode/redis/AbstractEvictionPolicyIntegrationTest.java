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

package org.apache.geode.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public abstract class AbstractEvictionPolicyIntegrationTest implements RedisPortSupplier {
  public static final String MAXMEMORY_FIELD = "maxmemory:";
  public static final String USED_MEMORY_FIELD = "used_memory:";
  private Jedis jedis;
  private Long maxMemory = 0L;
  private Long usedMemory = 0L;
  Long estimatedSizePerKey = 0L;
  private Long targetNumberOfKeysToAdd = 0L;
  private String oneKilobyteString = ""; // Initialized in setUp()
  private static String KEY_BASE = "testKey-";
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static Long NUM_REMAINING_KEYS = 10L;
  private Set<String> expectedKeys = new HashSet<>();
  Logger logger = LogService.getLogger();

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  public abstract void configureMemoryAndEvictionPolicy(Jedis jedis);

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);

    configureMemoryAndEvictionPolicy(jedis);

    oneKilobyteString = generateOneKilobyteLongString();
    maxMemory = getMemoryInfoValue(MAXMEMORY_FIELD);

    estimatedSizePerKey = estimateSizePerKey();
    logger.info("estimated size:" + estimatedSizePerKey);

    usedMemory = getMemoryInfoValue(USED_MEMORY_FIELD);

    targetNumberOfKeysToAdd =
        ((maxMemory - usedMemory) / estimatedSizePerKey) - NUM_REMAINING_KEYS;

    logger.info("max:" + maxMemory + " used:" + usedMemory +
        " targetNumber:" + targetNumberOfKeysToAdd);

    for (int i = 0; i < targetNumberOfKeysToAdd; i++) {
      jedis.set(KEY_BASE + i, oneKilobyteString);
      expectedKeys.add(KEY_BASE + i);
    }

    usedMemory = getMemoryInfoValue(USED_MEMORY_FIELD);
    logger.info("post add, max:" + maxMemory + " used:" + usedMemory);

    assertThat((maxMemory - usedMemory) / estimatedSizePerKey)
        .isGreaterThanOrEqualTo(NUM_REMAINING_KEYS);
    assertThat(expectedKeys).containsAll(jedis.keys("*"));
    logger.info("setup added " + expectedKeys.size() + " keys, remaining memory:"
        + (maxMemory - usedMemory));
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
  }

  @Test
  public void whenBelowMaxMemory_KeysAreNotEvicted_onNewKeyAddition() {
    int previousKeyCount = jedis.keys("*").size();
    Long previousEvictionCount = getNumEvictedKeys();

    jedis.set(KEY_BASE + expectedKeys.size() + 1, oneKilobyteString);

    assertThat(getNumEvictedKeys()).isEqualTo(previousEvictionCount);
    assertThat(jedis.keys("*").size()).isEqualTo(previousKeyCount + 1);
  }

  @Test
  @Ignore("Geode doesn't support Redis-style eviction (yet?)")
  public void whenMaxMemoryExceeded_KeysAreEvicted_onNewKeyAddition() {
    Set<String> preEvictionKeySet = jedis.keys("*");
    Long previousEvictions = getNumEvictedKeys();

    logger.info("after setup(), allKeys size:" + preEvictionKeySet.size()
        + "; previousEvictions:" + previousEvictions);

    int i = 0;
    while (maxMemory - getMemoryInfoValue(USED_MEMORY_FIELD) > estimatedSizePerKey) {
      String newKey = "extraKey" + i;
      i++;
      jedis.set(newKey, oneKilobyteString);
      preEvictionKeySet.add(newKey);
    }
    logger.info("after nearloop, keys(*) size:" + jedis.keys("*").size()
        + "; added " + i + " keys");

    logger.info("**** num evicted:" + getNumEvictedKeys());
    assertThat(getNumEvictedKeys()).isEqualTo(previousEvictions);

    jedis.set("strawThatBrokeTheCamelsBack", oneKilobyteString + oneKilobyteString);
    assertNotNull("straw not present!", jedis.get("strawThatBrokeTheCamelsBack"));
    logger.info("**** num evicted:" + getNumEvictedKeys());
    assertThat(getNumEvictedKeys()).isGreaterThan(previousEvictions);

    Set<String> allKeys = jedis.keys("*");
    assertThat(allKeys).contains("strawThatBrokeTheCamelsBack");
    preEvictionKeySet.removeAll(allKeys);
    assertThat(preEvictionKeySet.size()).isGreaterThan(0);
  }

  private Long getMemoryInfoValue(String infoField) {
    String valueString = "";
    String memoryInfo = jedis.info("memory");
    String[] parsedInfo = memoryInfo.split("\r\n");
    for (int i = 0; i < parsedInfo.length; i++) {
      if (parsedInfo[i].contains(infoField)) {
        valueString = parsedInfo[i].substring(parsedInfo[i].indexOf(':') + 1);
        break;
      }
    }
    // TODO: Geode Redis probably needs to support the "maxmemory:" field
    if (valueString.equals("")) {
      return 2000000L;
    }
    return Long.valueOf(valueString);
  }

  private Long getNumEvictedKeys() {
    String valueString = "";
    String memoryInfo = jedis.info("stats");
    String[] parsedInfo = memoryInfo.split("\r\n");
    for (int i = 0; i < parsedInfo.length; i++) {
      if (parsedInfo[i].contains("evicted_keys:")) {
        valueString = parsedInfo[i].substring(parsedInfo[i].indexOf(':') + 1);
        break;
      }
    }
    return Long.valueOf(valueString);
  }

  private String generateOneKilobyteLongString() {
    int length = 1024;
    boolean useLetters = true;
    boolean useNumbers = false;
    return RandomStringUtils.random(length, useLetters, useNumbers);
  }

  private Long estimateSizePerKey() {
    Long initialMemoryUsage = getMemoryInfoValue(USED_MEMORY_FIELD);
    jedis.set(KEY_BASE, oneKilobyteString);
    Long postAddMemoryUsage = getMemoryInfoValue(USED_MEMORY_FIELD);
    jedis.del(KEY_BASE);
    return postAddMemoryUsage - initialMemoryUsage;
  }
}
