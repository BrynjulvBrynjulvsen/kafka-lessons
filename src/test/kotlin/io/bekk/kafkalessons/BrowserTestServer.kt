package io.bekk.kafkalessons

import org.springframework.boot.runApplication
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker

/** Isolated real Kafka for browser verification; never connects to the workshop or POC. */
fun main(args: Array<String>) {
    val broker = EmbeddedKafkaKraftBroker(1, 3, "kafka-demo", "kafka-demo-lab")
    broker.brokerListProperty("spring.kafka.bootstrap-servers")
    broker.afterPropertiesSet()
    Runtime.getRuntime().addShutdownHook(Thread { broker.destroy() })
    try { runApplication<Application>(*args) }
    catch (error: Throwable) { broker.destroy(); throw error }
}
