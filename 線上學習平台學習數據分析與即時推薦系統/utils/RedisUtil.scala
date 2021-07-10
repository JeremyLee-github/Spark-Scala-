package cn.it.edu.utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/*
redis工具列
 */

object RedisUtil {
  val host = "localhost"
  val port = 6397
  val timeout = 30000
  val config: JedisPoolConfig = new JedisPoolConfig
  config.setMaxTotal(200)
  config.setMaxIdle(50)
  config.setMinIdle(8)
  config.setMaxWaitMillis(10000)
  config.setTestOnBorrow(true)
  config.setTestOnReturn(true)
  config.setTestWhileIdle(true)
  config.setTimeBetweenEvictionRunsMillis(30000)
  config.setNumTestsPerEvictionRun(10)
  config.setMinEvictableIdleTimeMillis(60000)

  //
  lazy val pool: JedisPool = new JedisPool(config, host, port, timeout)

  //釋放資源
  lazy val hook: Thread = new Thread {
    override def run() = {
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}
