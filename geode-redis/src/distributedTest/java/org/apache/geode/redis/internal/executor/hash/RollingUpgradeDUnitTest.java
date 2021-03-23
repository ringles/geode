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
 *
 */

package org.apache.geode.redis.internal.executor.hash;

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisException;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.resource.ClientResources;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.session.springRedisTestApplication.config.DUnitSocketAddressResolver;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class RollingUpgradeDUnitTest {

  private static final Logger logger = LogService.getLogger();

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule(5);

  private static Properties locatorProperties;

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;
  private static MemberVM server4;

  private static String[] redisPortStrings = new String[4];

  private StatefulRedisConnection<String, String>[] connections = new StatefulRedisConnection[4];
  private RedisClient[] redisClients = new RedisClient[4];

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @BeforeClass
  public static void classSetup() {
    int[] redisPorts = AvailablePortHelper.getRandomAvailableTCPPorts(4);
    for (int i = 0; i < 4; i++) {
      redisPortStrings[i] = "" + redisPorts[i];
    }

    locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    locator = cluster.startLocatorVM(0, locatorProperties);
    server1 = cluster.startServerVM(1, locator.getPort());
    server2 = cluster.startServerVM(2, locator.getPort());
    server3 = cluster.startServerVM(3, locator.getPort());
    server4 = cluster.startServerVM(4, locator.getPort());
  }

  @Before
  public void before() {
    addIgnoredException(FunctionException.class);

    DUnitSocketAddressResolver dnsResolver1 =
        new DUnitSocketAddressResolver(new String[]{redisPortStrings[0]});
    DUnitSocketAddressResolver dnsResolver2 =
        new DUnitSocketAddressResolver(new String[]{redisPortStrings[1]});
    DUnitSocketAddressResolver dnsResolver3 =
        new DUnitSocketAddressResolver(new String[]{redisPortStrings[2]});
    DUnitSocketAddressResolver dnsResolver4 =
        new DUnitSocketAddressResolver(new String[]{redisPortStrings[3]});

    ClientResources resources1 = ClientResources.builder()
        .socketAddressResolver(dnsResolver1)
        .build();
    redisClients[0] = RedisClient.create(resources1, "redis://localhost");
    redisClients[0].setOptions(ClientOptions.builder()
        .autoReconnect(true)
        .build());

    ClientResources resources2 = ClientResources.builder()
        .socketAddressResolver(dnsResolver2)
        .build();
    redisClients[1] = RedisClient.create(resources2, "redis://localhost");
    redisClients[1].setOptions(ClientOptions.builder()
        .autoReconnect(true)
        .build());

    ClientResources resources3 = ClientResources.builder()
        .socketAddressResolver(dnsResolver3)
        .build();
    redisClients[2] = RedisClient.create(resources3, "redis://localhost");
    redisClients[2].setOptions(ClientOptions.builder()
        .autoReconnect(true)
        .build());

    ClientResources resources4 = ClientResources.builder()
        .socketAddressResolver(dnsResolver4)
        .build();
    redisClients[3] = RedisClient.create(resources4, "redis://localhost");
    redisClients[3].setOptions(ClientOptions.builder()
        .autoReconnect(true)
        .build());
  }

  @After
  public void after() {
    for (int i = 0; i < 4; i++) {
      connections[i].close();
      redisClients[i].shutdown();
    }
  }

  @Test
  public void givenServerCrashesDuringHSET_thenDataIsNotLost_andNoExceptionsAreLogged()
      throws Exception {
    modifyDataWhileCrashingVMs(DataType.HSET);
  }

  @Test
  public void givenServerCrashesDuringSADD_thenDataIsNotLost() throws Exception {
    modifyDataWhileCrashingVMs(DataType.SADD);
  }

  @Test
  public void givenServerCrashesDuringSET_thenDataIsNotLost() throws Exception {
    modifyDataWhileCrashingVMs(DataType.SET);
  }

  enum DataType {
    HSET, SADD, SET
  }

  private void modifyDataWhileCrashingVMs(DataType dataType) throws Exception {
    AtomicBoolean running1 = new AtomicBoolean(true);
    AtomicBoolean running2 = new AtomicBoolean(true);
    AtomicBoolean running3 = new AtomicBoolean(true);
    AtomicBoolean running4 = new AtomicBoolean(true);

    Runnable task1 = null;
    Runnable task2 = null;
    Runnable task3 = null;
    Runnable task4 = null;

    switch (dataType) {
      case HSET:
        task1 = () -> hsetPerformAndVerify(0, running1);
        task2 = () -> hsetPerformAndVerify(1, running2);
        task3 = () -> hsetPerformAndVerify(2, running3);
        task4 = () -> hsetPerformAndVerify(3, running4);
        break;
      case SADD:
        task1 = () -> saddPerformAndVerify(0, running1);
        task2 = () -> saddPerformAndVerify(1, running2);
        task3 = () -> saddPerformAndVerify(2, running3);
        task4 = () -> saddPerformAndVerify(3, running4);
        break;
      case SET:
        task1 = () -> setPerformAndVerify(0, running1);
        task2 = () -> setPerformAndVerify(1, running2);
        task3 = () -> setPerformAndVerify(2, running3);
        task4 = () -> setPerformAndVerify(3, running4);
        break;
    }

    server1.stop();
    server1 = cluster.startRedisVM(1, redisPortStrings[0], locator.getPort());
    rebalanceAllRegions(server1);
//    Thread.sleep(2000);
    Future<Void> future1 = executor.runAsync(task1);

    server2.stop();
    server2 = cluster.startRedisVM(2, redisPortStrings[1], locator.getPort());
    rebalanceAllRegions(server2);
//    Thread.sleep(2000);
    Future<Void> future2 = executor.runAsync(task2);

    server3.stop();
    server3 = cluster.startRedisVM(3, redisPortStrings[2], locator.getPort());
    rebalanceAllRegions(server3);
//    Thread.sleep(2000);
    Future<Void> future3 = executor.runAsync(task3);

    server4.stop();
    server4 = cluster.startRedisVM(4, redisPortStrings[3], locator.getPort());
    rebalanceAllRegions(server4);
    Future<Void> future4 = executor.runAsync(task4);
    Thread.sleep(2000);

    running1.set(false);
    running2.set(false);
    running3.set(false);
    running4.set(false);

    future1.get();
    future2.get();
    future3.get();
    future4.get();
  }

  private void hsetPerformAndVerify(int index, AtomicBoolean isRunning) {
    String key = "hset-key-" + index;
    int iterationCount = 0;

    connections[index] = redisClients[index].connect();
    RedisCommands<String, String> commands = connections[index].sync();

    while (isRunning.get()) {
      String fieldName = "field-" + iterationCount;
      try {
        commands.hset(key, fieldName, "value-" + iterationCount);
        iterationCount += 1;
      } catch (RedisCommandExecutionException ignore) {
      } catch (RedisException ex) {
        if (ex.getMessage().contains("Connection reset by peer")) {
          // ignore it
        } else {
          throw ex;
        }
      }
    }

    for (int i = 0; i < iterationCount; i++) {
      String field = "field-" + i;
      String value = "value-" + i;
      assertThat(commands.hget(key, field)).isEqualTo(value);
    }

    logger.info("--->>> HSET test index {} ran {} iterations", index, iterationCount);
  }

  private void saddPerformAndVerify(int index, AtomicBoolean isRunning) {
    String key = "sadd-key-" + index;
    int iterationCount = 0;

    connections[index] = redisClients[index].connect();
    RedisCommands<String, String> commands = connections[index].sync();

    while (isRunning.get()) {
      String member = "member-" + index + "-" + iterationCount;
      try {
        commands.sadd(key, member);
        iterationCount += 1;
      } catch (RedisCommandExecutionException ignore) {
      } catch (RedisException ex) {
        if (ex.getMessage().contains("Connection reset by peer")) {
          // ignore it
        } else {
          throw ex;
        }
      }
    }

    List<String> missingMembers = new ArrayList<>();
    for (int i = 0; i < iterationCount; i++) {
      String member = "member-" + index + "-" + i;
      if (!commands.sismember(key, member)) {
        missingMembers.add(member);
      }
    }
    assertThat(missingMembers).isEmpty();

    logger.info("--->>> SADD test index {}  ran {} iterations", index, iterationCount);
  }

  private void setPerformAndVerify(int index, AtomicBoolean isRunning) {
    int iterationCount = 0;

    connections[index] = redisClients[index].connect();
    RedisCommands<String, String> commands = connections[index].sync();

    while (isRunning.get()) {
      String key = "set-key-" + index + "-" + iterationCount;
      try {
        commands.set(key, key);
        iterationCount += 1;
      } catch (RedisCommandExecutionException ignore) {
      } catch (RedisException ex) {
        if (ex.getMessage().contains("Connection reset by peer")) {
          // ignore it
        } else {
          throw ex;
        }
      }
    }

    for (int i = 0; i < iterationCount; i++) {
      String key = "set-key-" + index + "-" + i;
      String value = commands.get(key);
      assertThat(value).isEqualTo(key);
    }

    logger.info("--->>> SET test index {} ran {} iterations", index, iterationCount);
  }

  private static void rebalanceAllRegions(MemberVM vm) {
    vm.invoke(() -> {
      ResourceManager manager = ClusterStartupRule.getCache().getResourceManager();

      RebalanceFactory factory = manager.createRebalanceFactory();

      try {
        RebalanceResults result = factory.start().getResults();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
