package io.bekk.kafkalessons

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ConsumerFactory
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import tools.jackson.databind.ObjectMapper
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.web.server.ResponseStatusException
import java.time.Duration
import java.util.concurrent.TimeUnit

@SpringBootTest(properties = [
    "demo.topics=notifications-test,confirmations-test", "demo.default-topic=notifications-test",
    "demo.notifications.confirmation-topic=confirmations-test",
    "spring.kafka.consumer.group-id=notification-test-observer",
    "demo.experiment.topic=notifications-test", "demo.experiment.group-prefix=notification-test",
])
@EmbeddedKafka(partitions = 3, topics = ["notifications-test", "confirmations-test"], bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@DirtiesContext
class NotificationIntegrationTest {
    @Autowired private lateinit var runtime: NotificationRuntime
    @Autowired private lateinit var producer: KafkaTemplate<String, String>
    @Autowired private lateinit var factory: ConsumerFactory<String, String>
    @Autowired private lateinit var json: ObjectMapper
    @Suppress("UNCHECKED_CAST")
    private fun orders() = runtime.snapshot()["orders"] as List<Map<String, Any?>>
    private fun until(assertion: () -> Unit) = await().atMost(Duration.ofSeconds(40)).untilAsserted { assertion() }

    @Test
    fun `explicit chain publishes correlated confirmation records and consumes them into a bounded inbox`() {
        assertThat(runtime.snapshot()["state"]).isEqualTo("stopped")
        assertThatThrownBy { runtime.command(NotificationCommand("place", "order-42")) }.isInstanceOf(ResponseStatusException::class.java)
        runtime.command(NotificationCommand("start"))
        until { assertThat(runtime.snapshot()["state"]).isEqualTo("running") }
        assertThatThrownBy { runtime.command(NotificationCommand("clear")) }.isInstanceOf(ResponseStatusException::class.java)
        assertThatThrownBy { runtime.command(NotificationCommand("place", "<script>")) }.isInstanceOf(ResponseStatusException::class.java)
        for (value in listOf("not json", "null", "{\"type\":\"OrderPlaced\"}"))
            producer.send(runtime.topic, "unrelated", value).get(10, TimeUnit.SECONDS)
        runtime.command(NotificationCommand("place", "order-42"))
        until { assertThat(orders().single()["completedAt"]).isNotNull() }
        val first = orders().single()
        assertThat(first["acknowledgedAt"]).isNotNull()
        assertThat(first["receivedAt"]).isNotNull()
        assertThat(first["confirmationAcknowledgedAt"]).isNotNull()
        assertThat(first["notification"]).isEqualTo("Order order-42 received. Your confirmation is ready.")
        assertThat(first["partition"]).isIn(0, 1, 2)
        assertThat(first["offset"]).isInstanceOf(Long::class.javaObjectType)
        assertThat(first["inboxPartition"]).isEqualTo(first["confirmationPartition"])
        assertThat(first["inboxOffset"]).isEqualTo(first["confirmationOffset"])
        // Independently read the output topic: the result really crossed Kafka.
        val settings = factory.configurationProperties.toMutableMap().apply {
            put(ConsumerConfig.GROUP_ID_CONFIG, "confirmation-audit")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        }
        KafkaConsumer<String, String>(settings).use { audit ->
            val partition = TopicPartition(runtime.confirmationTopic, first["confirmationPartition"] as Int)
            audit.assign(listOf(partition)); audit.seek(partition, first["confirmationOffset"] as Long)
            until {
                val records = audit.poll(Duration.ofMillis(200))
                assertThat(records.count()).isGreaterThan(0)
                val record = records.first()
                val event = json.readTree(record.value())
                assertThat(record.key()).isEqualTo("order-42")
                assertThat(event.path("type").asString()).isEqualTo("OrderConfirmationPrepared")
                assertThat(event.path("causationId").asString()).isEqualTo(first["eventId"])
                assertThat(event.path("eventId").asString()).isEqualTo(first["confirmationEventId"])
                assertThat(event.path("message").asString()).isEqualTo(first["notification"])
            }
        }
        // An event with a valid marker but no locally submitted correlation must not create an effect.
        producer.send(runtime.topic, "order-forged", """{"type":"OrderPlaced","demo":"notifications","eventId":"unknown","orderId":"order-forged"}""").get(10, TimeUnit.SECONDS)
        producer.send(runtime.confirmationTopic, "order-forged", """{"type":"OrderConfirmationPrepared","demo":"notifications","eventId":"unknown","causationId":"unknown","orderId":"order-forged","message":"forged"}""").get(10, TimeUnit.SECONDS)
        repeat(12) { index ->
            runtime.command(NotificationCommand("place", "order-$index"))
            until { assertThat(orders().last()["completedAt"]).isNotNull() }
        }
        assertThat(orders()).hasSize(12)
        assertThat(orders().map { it["orderId"] }).doesNotContain("order-42", "order-forged")
        runtime.command(NotificationCommand("stop"))
        until { assertThat(runtime.snapshot()["state"]).isEqualTo("stopped") }
        runtime.command(NotificationCommand("clear"))
        assertThat(orders()).isEmpty()
        assertThat(runtime.snapshot()["state"]).isEqualTo("stopped")
        runtime.command(NotificationCommand("start"))
        until { assertThat(runtime.snapshot()["state"]).isEqualTo("running") }
        runtime.command(NotificationCommand("place", "after-restart"))
        until { assertThat(orders().single()["completedAt"]).isNotNull() }
        runtime.command(NotificationCommand("stop"))
        until { assertThat(runtime.snapshot()["state"]).isEqualTo("stopped") }
    }
}
