package io.bekk.kafkalessons

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.kafka.autoconfigure.KafkaProperties
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import java.nio.file.Files
import java.nio.file.Path

class PocLegacyConfigurationTest {
    @TempDir lateinit var directory: Path

    @Test fun `legacy profile shares authentication across lesson clients without changing observer ownership`() {
        val client = directory.resolve("client.properties")
        val jaas = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"fixture\" password=\"fixture\";"
        Files.writeString(client, "sasl.jaas.config=$jaas\ngroup.id=unrelated-poc-group\nbootstrap.servers=wrong:9092\n")
        ApplicationContextRunner()
            // EmbeddedKafka exposes its address as a JVM property; this fixture tests YAML defaults.
            .withInitializer { it.environment.propertySources.remove("systemProperties") }
            .withInitializer(ConfigDataApplicationContextInitializer())
            .withPropertyValues("spring.profiles.active=poc-legacy", "KAFKA_CLIENT_PROPERTIES=$client")
            .run { context ->
                assertThat(context).hasNotFailed()
                val kafka = Binder.get(context.environment).bind("spring.kafka", KafkaProperties::class.java).get()
                for (properties in listOf(kafka.buildConsumerProperties(), kafka.buildProducerProperties(), kafka.buildAdminProperties())) {
                    assertThat(properties["bootstrap.servers"]).isEqualTo(listOf("localhost:32095"))
                    assertThat(properties["security.protocol"]).isEqualTo("SASL_PLAINTEXT")
                    assertThat(properties["sasl.mechanism"]).isEqualTo("SCRAM-SHA-512")
                    assertThat(properties["sasl.jaas.config"]).isEqualTo(jaas)
                }
                assertThat(kafka.buildConsumerProperties()["group.id"]).isEqualTo("kafka-demo-poc-observer")
                assertThat(kafka.buildConsumerProperties()["enable.auto.commit"]).isEqualTo(false)
                assertThat(context.environment.getProperty("demo.experiment.enabled")).isEqualTo("true")
                assertThat(context.environment.getProperty("demo.experiment.group-prefix")).isEqualTo("kafka-demo-poc-experiment")
            }
    }
}
