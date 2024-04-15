package no.nav.modiaeventdistribution.redis

import no.nav.common.utils.EnvironmentUtils
import redis.clients.jedis.HostAndPort

fun setupRedis(): Redis.Consumer {
    return Redis.Consumer(
        hostAndPort = HostAndPort(EnvironmentUtils.getRequiredProperty("REDIS_HOST"), 6379),
        password = EnvironmentUtils.getRequiredProperty("REDIS_PASSWORD")
    )
}