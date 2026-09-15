package io.bekk.kafkalessons

import io.bekk.kafkademo.core.*
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Import


@SpringBootApplication
@ConfigurationPropertiesScan
@Import(KafkaConnectionsConfiguration::class, KafkaProducerConfiguration::class, KafkaObserverConfiguration::class)
class Application {

}

fun main(args: Array<String>) { runApplication<Application>(*args) }
