package io.bekk.kafkalessons

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.web.server.ResponseStatusException
import java.time.Duration
import java.util.concurrent.TimeUnit

@SpringBootTest(properties = [
    "demo.topics=experiment-test,experiment-confirmations", "demo.default-topic=experiment-test",
    "demo.notifications.confirmation-topic=experiment-confirmations",
    "spring.kafka.consumer.group-id=experiment-observer-test",
    "demo.experiment.enabled=true", "demo.experiment.topic=experiment-test",
    "demo.experiment.group-prefix=experiment-integration",
])
@EmbeddedKafka(partitions = 3, topics = ["experiment-test", "experiment-confirmations"], bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@DirtiesContext
class ExperimentIntegrationTest {
    @Autowired private lateinit var runtime: ExperimentRuntime
    @Autowired private lateinit var producer: KafkaTemplate<String, String>

    @Suppress("UNCHECKED_CAST")
    private fun rows(name: String) = runtime.snapshot()[name] as List<Map<String, Any?>>
    private fun until(assertion: () -> Unit) = await().atMost(Duration.ofSeconds(45)).untilAsserted { assertion() }
    private fun send(partition: Int, value: String) = producer.send("experiment-test", partition, "key-$partition", value).get(10, TimeUnit.SECONDS)
    private fun stop(group: String) {
        runtime.command(ExperimentCommand("stop", group))
        until { assertThat(rows("members").filter { it["group"] == group }.all { it["state"] == "stopped" }).isTrue() }
    }

    @Test
    fun `groups split ownership, replay is explicit, and slow processing builds recoverable lag`() {
        val a = runtime.groups[0]; val b = runtime.groups[1]
        until { assertThat(runtime.snapshot()["partitions"]).isEqualTo(listOf(0, 1, 2)) }
        repeat(3) { send(it, "initial") }
        runtime.command(ExperimentCommand("start", a))
        until { assertThat(rows("offsets").filter { it["group"] == a }.map { it["committed"] }).containsExactly(1L, 1L, 1L) }
        repeat(3) { runtime.command(ExperimentCommand("start", a)) }
        until {
            val members = rows("members").filter { it["group"] == a }
            assertThat(members).hasSize(4)
            assertThat(members.map { it["state"] }).containsOnly("running")
            val assignments = members.map { it["assignment"] as List<*> }
            assertThat(assignments.flatten()).containsExactlyInAnyOrder(0, 1, 2)
            assertThat(assignments.count { it.isEmpty() }).isEqualTo(1)
        }
        assertThatThrownBy { runtime.command(ExperimentCommand("start", a)) }.isInstanceOf(ResponseStatusException::class.java)
        assertThatThrownBy { runtime.command(ExperimentCommand("reset", a)) }.isInstanceOf(ResponseStatusException::class.java)
        runtime.command(ExperimentCommand("start", b, policy = "latest"))
        until { assertThat(rows("members").first { it["group"] == b }["assignment"]).isEqualTo(listOf(0, 1, 2)) }
        // Position confirms latest has been resolved, not merely that assignment callback fired.
        until { assertThat((rows("members").first { it["group"] == b }["position"] as Map<*, *>).size).isEqualTo(3) }
        repeat(3) { send(it, "both groups") }
        until { assertThat(rows("offsets").map { it["committed"] }).containsOnly(2L) }
        stop(a)
        assertThatThrownBy { runtime.command(ExperimentCommand("reset", a, offset = 999999)) }.isInstanceOf(ResponseStatusException::class.java)
        runtime.command(ExperimentCommand("reset", a, partition = 0, offset = 0))
        val eventBefore = rows("events").last()["id"] as Long
        runtime.command(ExperimentCommand("start", a, policy = "latest"))
        until {
            assertThat(rows("events").any { it["kind"] == "delivered" && it["group"] == a && it["partition"] == 0 && it["offset"] == 0L && (it["id"] as Long) > eventBefore }).isTrue()
        }
        until { assertThat(rows("offsets").map { it["committed"] }).containsOnly(2L) }
        stop(a)
        val restartAfter = rows("events").last()["id"] as Long
        runtime.command(ExperimentCommand("start", a, policy = "earliest"))
        until { assertThat(rows("members").first { it["group"] == a }["position"] as Map<*, *>).hasSize(3) }
        assertThat(rows("events").filter { (it["id"] as Long) > restartAfter && it["kind"] == "delivered" && it["group"] == a }).isEmpty()
        runtime.command(ExperimentCommand("delay", a, delayMs = 1000))
        assertThatThrownBy { runtime.command(ExperimentCommand("delay", delayMs = 200)) }.isInstanceOf(ResponseStatusException::class.java)
        assertThat((runtime.snapshot()["groupDelays"] as Map<*, *>)[b]).isEqualTo(0L)
        repeat(12) { send(it % 3, "lag-$it") }
        until {
            assertThat(rows("offsets").filter { it["group"] == a }.any { (it["lag"] as? Long ?: 0) > 0 }).isTrue()
            assertThat(rows("offsets").filter { it["group"] == b }.map { it["lag"] }).containsOnly(0L)
        }
        runtime.command(ExperimentCommand("delay", a, delayMs = 0))
        until { assertThat(rows("offsets").map { it["lag"] }).containsOnly(0L) }
        stop(a); stop(b)
        stop(a); stop(b) // repeated stop must preserve the stopped state
        assertThat(rows("events").size).isLessThanOrEqualTo(48)
    }
}
