package no.nav.modiaeventdistribution.redis

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.consumeAsFlow
import no.nav.common.health.HealthCheckResult
import no.nav.common.health.selftest.SelfTestCheck
import no.nav.common.utils.EnvironmentUtils
import no.nav.modiaeventdistribution.infrastructur.HealthCheckAware
import no.nav.modiaeventdistribution.log
import redis.clients.jedis.HostAndPort
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPubSub

object Redis {
    private val environment = EnvironmentUtils.getRequiredProperty("APP_ENVIRONMENT")

    @JvmStatic
    fun getChannel() = "ContextOppdatering-$environment"

    class Consumer(
        private val hostAndPort: HostAndPort,
        private val password: String,
        private val channel: String = getChannel(),
    ) : HealthCheckAware {

        private var running = false
        private var thread: Thread? = null
        private var jedis: Jedis? = null
        private val channelReference = Channel<String?>()

        private val subscriber = object : JedisPubSub() {
            override fun onMessage(channel: String?, message: String?) {
                runBlocking {
                    channelReference.send(message)
                }
                log.info(
                    """
                        Redismelding mottatt på kanal '$channel' med melding:
                        $message
                    """.trimIndent(),
                )
            }
        }

        fun getFlow() = channelReference.consumeAsFlow()

        fun start() {
            running = true
            log.info("starting redis consumer on channel '$channel'")

            thread = Thread(Runnable { run() })
            thread?.name = "consumer-$channel"
            thread?.isDaemon = true
            thread?.start()
        }

        fun stop() {
            running = false
            subscriber.unsubscribe()
            channelReference.close()
            thread = null
        }

        private fun run() {
            while (running) {
                try {
                    jedis = Jedis(hostAndPort)
                    jedis?.auth(password)
                    jedis?.subscribe(subscriber, channel)
                } catch (e: Exception) {
                    log.error(e.message, e)
                }
            }
        }

        private fun checkHealth(): HealthCheckResult {
            return try {
                jedis?.ping()
                HealthCheckResult.healthy()
            } catch (e: Exception) {
                HealthCheckResult.unhealthy(e)
            }
        }

        override fun getHealthCheck(): SelfTestCheck {
            return SelfTestCheck(
                "Redis - via ${hostAndPort.host}",
                false,
            ) { this.checkHealth() }
        }
    }
}
