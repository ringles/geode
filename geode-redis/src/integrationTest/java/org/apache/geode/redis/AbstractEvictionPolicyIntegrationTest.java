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
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.InstanceOfAssertFactories.BOOLEAN_ARRAY;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public abstract class AbstractEvictionPolicyIntegrationTest implements RedisPortSupplier {
  private Jedis jedis;
  private Long maxMemory = 0L;
  private Long usedMemory = 0L;
  Long targetNumberOfKeysToAdd = 0L;
  private String oneKilobyteString = ""; // Initialized in setUp()
  private static String KEY_BASE = "testKey-";
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static Long OVERHEAD_FACTOR = 10L;
  private static Long NUM_REMAINING_KEYS = 10L;

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
    oneKilobyteString = generateOneKilobyteLongString();

    String maxMemoryString = "";
    String usedMemoryString = "";
    String infoStuff = jedis.info("memory");
    String[] parsedInfo = infoStuff.split("\r\n");
    for (int i= 0; i<parsedInfo.length; i++) {
      if (parsedInfo[i].contains("maxmemory:")) {
        maxMemoryString = parsedInfo[i].substring(parsedInfo[i].indexOf(':')+1);
      }
      if (parsedInfo[i].contains("used_memory:")) {
        usedMemoryString = parsedInfo[i].substring(parsedInfo[i].indexOf(':')+1);
      }
      // TODO: get lazy?
      if (maxMemoryString.length() > 0 && usedMemoryString.length() > 0) {
        break;
      }
    }

    maxMemory = Long.valueOf(maxMemoryString);
    usedMemory = Long.valueOf(usedMemoryString);

    Long remainingMemoryInKB = (maxMemory - usedMemory) / 1024;
    targetNumberOfKeysToAdd = remainingMemoryInKB - OVERHEAD_FACTOR - NUM_REMAINING_KEYS;

    for (int i = 0; i < targetNumberOfKeysToAdd; i++) {
      jedis.set(KEY_BASE + i, oneKilobyteString);
    }
  }

  @After
  public void tearDown() {
    jedis.close();
  }

  @Test
  public void whenBelowMaxMemory_KeysAreNotEvicted_onNewKeyAddition() {
    // add some small keys
    assertThatNoException().isThrownBy(() -> {
      for (long i = targetNumberOfKeysToAdd; i < targetNumberOfKeysToAdd + NUM_REMAINING_KEYS; i++) {
        jedis.set(KEY_BASE + i, oneKilobyteString);
      }
    });

    // confirm all expected keys still present
    for (int i = 0; i < targetNumberOfKeysToAdd + NUM_REMAINING_KEYS; i++) {
      assertThat(jedis.get(KEY_BASE + i).equals(oneKilobyteString));
    }
  }


  private String generateOneKilobyteLongString() {
    int length = 1024;
    boolean useLetters = true;
    boolean useNumbers = false;
    return RandomStringUtils.random(length, useLetters, useNumbers);
  }
}
