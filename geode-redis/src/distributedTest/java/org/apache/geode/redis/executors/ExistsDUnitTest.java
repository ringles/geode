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

import java.io.Serializable;
import java.net.HttpCookie;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;
import redis.clients.jedis.Jedis;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.springRedisTestApplication.RedisSpringTestApplication;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class ExistsDUnitTest implements Serializable {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(3);

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  public static ConfigurableApplicationContext springApplicationContext;

  private static final int LOCATOR = 0;
  private static final int SERVER1 = 1;
  private static final int SERVER2 = 2;
  private static final Map<Integer, Integer> ports = new HashMap<>();

  private static Jedis jedis;
  private static Jedis jedis2;
  private static final int JEDIS_TIMEOUT = Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @BeforeClass
  public static void setup() {
    int[] availablePorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    ports.put(SERVER1, availablePorts[0]);
    ports.put(SERVER2, availablePorts[1]);

    cluster.startLocatorVM(LOCATOR);
    startRedisServer(SERVER1);
    startRedisServer(SERVER2);

    jedis = new Jedis("localhost", ports.get(SERVER1), JEDIS_TIMEOUT);
    jedis2 = new Jedis("localhost", ports.get(SERVER2), JEDIS_TIMEOUT);
  }

  @After
  public void cleanupAfterTest() {
    jedis.flushAll();
  }

  @AfterClass
  public static void cleanupAfterClass() {
    jedis.disconnect();
  }

  @Test
  public void testDistributedExistsOperations() {
    jedis.set("key", "value");
    assertThat(jedis2.exists("key")).isTrue();

    jedis2.del("key");
    assertThat(jedis.exists("key")).isFalse();
  }

  @Test
  public void should_existOnServer1_whenServer2GoesDown() {
    jedis2.set("key", "value");
    cluster.crashVM(SERVER2);
    try {
      assertThat(jedis.exists("key")).isTrue();
    } finally {
      startRedisServer(SERVER2);
    }
  }

  @Test
  public void should_existOnServer2_whenServerGoesDownAndIsRestarted() {
    jedis2.set("key2", "value2");
    cluster.crashVM(SERVER2);
    jedis.set("key1", "value1");
    startRedisServer(SERVER2);
    jedis2 = new Jedis("localhost", ports.get(SERVER2), JEDIS_TIMEOUT);

    assertThat(jedis2.exists("key1")).isTrue();
    assertThat(jedis2.exists("key2")).isTrue();
  }


  private static void startRedisServer(int server1) {
    cluster.startServerVM(server1, redisProperties(server1),
        cluster.getMember(LOCATOR).getPort());
  }

  private static Properties redisProperties(int server2) {
    Properties redisPropsForServer = new Properties();
    redisPropsForServer.setProperty("redis-bind-address", "localHost");
    redisPropsForServer.setProperty("redis-port", "" + ports.get(server2));
    redisPropsForServer.setProperty("log-level", "warn");
    return redisPropsForServer;
  }
}
