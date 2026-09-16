package io.bekk.kafkalessons

import io.bekk.kafkademo.core.DemoProperties
import jakarta.annotation.PreDestroy
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import org.springframework.web.server.ResponseStatusException
import tools.jackson.databind.ObjectMapper
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

/** Two Kafka hops: order -> confirmation event -> inbox, independently of observer cards. */
@Component
class NotificationRuntime(
    factory: ConsumerFactory<String, String>,
    private val producer: KafkaTemplate<String, String>,
    private val json: ObjectMapper,
    demo: DemoProperties,
    @param:Value("\${demo.experiment.enabled:true}") val enabled: Boolean,
    @param:Value("\${demo.experiment.topic:kafka-demo-lab}") val topic: String,
    @param:Value("\${demo.notifications.confirmation-topic:kafka-demo-confirmations}") val confirmationTopic: String,
    @Value("\${demo.experiment.group-prefix:kafka-demo-experiment}") prefix: String,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    val group = "$prefix-notifications"
    val inboxGroup = "$prefix-notification-inbox"
    private val properties = factory.configurationProperties.toMutableMap().apply {
        put(ConsumerConfig.GROUP_ID_CONFIG, group)
        put(ConsumerConfig.CLIENT_ID_CONFIG, "$group-service")
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20)
        put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000)
    }
    private val orders = linkedMapOf<String, MutableMap<String, Any?>>()
    private var state = "stopped"
    private var error: String? = null
    private var sending = false
    @Volatile private var running = false
    @Volatile private var closed = false
    @Volatile private var consumer: KafkaConsumer<String, String>? = null
    @Volatile private var inboxConsumer: KafkaConsumer<String, String>? = null
    private var serviceReady = false
    private var inboxReady = false
    private var worker: Thread? = null

    init {
        if (enabled) {
            require(topic in demo.topics) { "Notification topic must be configured" }
            require(confirmationTopic in demo.topics && confirmationTopic != topic) { "Configure a distinct confirmation topic" }
            require(group != factory.configurationProperties[ConsumerConfig.GROUP_ID_CONFIG]) { "Notification group must differ from observer" }
            require(inboxGroup != factory.configurationProperties[ConsumerConfig.GROUP_ID_CONFIG]) { "Inbox group must differ from observer" }
        }
    }

    @Synchronized fun snapshot(): Map<String, Any?> = mapOf(
        "enabled" to enabled, "topic" to topic, "group" to group,
        "confirmationTopic" to confirmationTopic, "inboxGroup" to inboxGroup,
        "state" to state, "error" to error, "at" to System.currentTimeMillis(),
        "sending" to sending, "orders" to orders.values.map { it.toMap() },
    )

    fun command(command: NotificationCommand): Map<String, Any?> {
        synchronized(this) {
            requireState(enabled && !closed, "Notification demo is disabled")
            when (command.action) {
                "start" -> {
                    requireState(worker?.isAlive != true, "Service is already active or stopping")
                    running = true; state = "starting"; error = null; serviceReady = false; inboxReady = false
                    worker = Thread({ consume() }, "notification-demo").apply { isDaemon = true; start() }
                }
                "stop" -> {
                    if (worker?.isAlive == true) {
                        state = "stopping"; running = false; consumer?.wakeup(); inboxConsumer?.wakeup()
                    }
                }
                "clear" -> {
                    requireState(!sending && worker?.isAlive != true, "Stop the service and wait before clearing the local inbox")
                    orders.clear() // Local inbox only; Kafka records and commits are unchanged.
                }
                "place" -> {} // Kafka send must not hold the state lock used by the consumer.
                else -> throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown notification action")
            }
        }
        if (command.action == "place") place(command.orderId)
        return snapshot()
    }

    private fun place(orderId: String?) {
        if (orderId == null || !orderId.matches(Regex("[A-Za-z0-9_-]{1,40}")))
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Order ID must be 1–40 letters, digits, dashes or underscores")
        val eventId = UUID.randomUUID().toString()
        synchronized(this) {
            requireState(state == "running" && running, "Start the service and wait for partition assignment")
            requireState(!sending && orders.values.none { it["completedAt"] == null && it["sendError"] == null },
                "Wait for the current order to finish, or stop the service and clear its inbox")
            sending = true
            while (orders.size >= 12) orders.remove(orders.keys.first())
            orders[eventId] = mutableMapOf("eventId" to eventId, "orderId" to orderId,
                "confirmationEventId" to "confirmation-$eventId", "confirmationAcknowledgedAt" to null,
                "submittedAt" to System.currentTimeMillis(), "acknowledgedAt" to null,
                "receivedAt" to null, "completedAt" to null, "notification" to null, "sendError" to null)
        }
        try {
            val payload = json.writeValueAsString(mapOf("type" to "OrderPlaced", "demo" to "notifications",
                "eventId" to eventId, "orderId" to orderId))
            val result = producer.send(topic, orderId, payload).get(12, TimeUnit.SECONDS).recordMetadata
            synchronized(this) {
                orders[eventId]?.putAll(mapOf("acknowledgedAt" to System.currentTimeMillis(),
                    "partition" to result.partition(), "offset" to result.offset()))
            }
        } catch (_: Exception) {
            synchronized(this) { orders[eventId]?.set("sendError", "No broker acknowledgment received; the write may have succeeded. No automatic retry.") }
        } finally { synchronized(this) { sending = false } }
    }

    private fun consume() {
        val inboxWorker = Thread({ consumeInbox() }, "notification-inbox-demo").apply { isDaemon = true; start() }
        try {
            KafkaConsumer<String, String>(properties).use { client ->
                consumer = client
                client.subscribe(listOf(topic))
                while (running) {
                    val records = client.poll(Duration.ofMillis(200))
                    if (client.assignment().isNotEmpty()) {
                        // Resolve initial positions before enabling production with latest.
                        client.assignment().forEach { client.position(it) }
                        synchronized(this) { serviceReady = true; markReady() }
                    }
                    for (record in records) {
                        val value = record.value() ?: continue
                        if (value.length > 16384) continue
                        val event = runCatching { json.readTree(value) }.getOrNull() ?: continue
                        if (event.path("demo").asString() != "notifications" || event.path("type").asString() != "OrderPlaced") continue
                        val id = event.path("eventId").asString()
                        val accepted = synchronized(this) {
                            val order = orders[id]
                            if (order == null || order["completedAt"] != null || order["orderId"] != event.path("orderId").asString() || record.key() != order["orderId"]) false
                            else {
                                order.putAll(mapOf("receivedAt" to System.currentTimeMillis(),
                                    "partition" to record.partition(), "offset" to record.offset()))
                                true
                            }
                        }
                        if (!accepted) continue
                        // Business processing produces another event; only the inbox consumer renders its result.
                        Thread.sleep(1500)
                        val confirmationId = "confirmation-$id"
                        val payload = json.writeValueAsString(mapOf("type" to "OrderConfirmationPrepared",
                            "demo" to "notifications", "eventId" to confirmationId, "causationId" to id,
                            "orderId" to event.path("orderId").asString(),
                            "message" to "Order ${event.path("orderId").asString()} received. Your confirmation is ready."))
                        val metadata = producer.send(confirmationTopic, record.key(), payload).get(12, TimeUnit.SECONDS).recordMetadata
                        synchronized(this) {
                            orders[id]?.putAll(mapOf("confirmationAcknowledgedAt" to System.currentTimeMillis(),
                                "confirmationPartition" to metadata.partition(), "confirmationOffset" to metadata.offset()))
                        }
                    }
                    if (!records.isEmpty) client.commitSync()
                }
            }
        } catch (_: WakeupException) {
            synchronized(this) { if (running) { state = "error"; error = "Consumer interrupted unexpectedly" } }
        } catch (failure: Exception) {
            log.warn("Notification demo consumer failed", failure)
            synchronized(this) { state = "error"; error = "Notification consumer failed. Check server logs and restart the service." }
        } finally {
            consumer = null; running = false; inboxConsumer?.wakeup(); inboxWorker.join()
            synchronized(this) { if (state != "error") state = "stopped" }
        }
    }

    // Separate consumer/group and thread: its only source of confirmation content is Kafka.
    private fun consumeInbox() {
        val settings = properties.toMutableMap().apply {
            put(ConsumerConfig.GROUP_ID_CONFIG, inboxGroup)
            put(ConsumerConfig.CLIENT_ID_CONFIG, "$inboxGroup-consumer")
        }
        try {
            KafkaConsumer<String, String>(settings).use { client ->
                inboxConsumer = client
                client.subscribe(listOf(confirmationTopic))
                while (running) {
                    val records = client.poll(Duration.ofMillis(200))
                    if (client.assignment().isNotEmpty()) {
                        client.assignment().forEach { client.position(it) }
                        synchronized(this) { inboxReady = true; markReady() }
                    }
                    for (record in records) {
                        val value = record.value() ?: continue
                        if (value.length > 16384) continue
                        val event = runCatching { json.readTree(value) }.getOrNull() ?: continue
                        if (event.path("demo").asString() != "notifications" || event.path("type").asString() != "OrderConfirmationPrepared") continue
                        synchronized(this) {
                            val order = orders[event.path("causationId").asString()]
                            if (order != null && order["completedAt"] == null &&
                                order["confirmationEventId"] == event.path("eventId").asString() &&
                                order["orderId"] == record.key() && order["orderId"] == event.path("orderId").asString() &&
                                event.path("message").isString) {
                                order.putAll(mapOf("notification" to event.path("message").asString(),
                                    "completedAt" to System.currentTimeMillis(), "inboxPartition" to record.partition(),
                                    "inboxOffset" to record.offset()))
                            }
                        }
                    }
                    if (!records.isEmpty) client.commitSync()
                }
            }
        } catch (_: WakeupException) {
            synchronized(this) { if (running) { state = "error"; error = "Inbox consumer interrupted unexpectedly" } }
        } catch (failure: Exception) {
            log.warn("Notification inbox consumer failed", failure)
            synchronized(this) { state = "error"; error = "Inbox consumer failed. Check server logs and restart the chain." }
        } finally {
            inboxConsumer = null; running = false; consumer?.wakeup()
        }
    }

    private fun markReady() {
        if (running && serviceReady && inboxReady) state = "running"
    }

    private fun requireState(valid: Boolean, message: String) {
        if (!valid) throw ResponseStatusException(HttpStatus.CONFLICT, message)
    }

    @PreDestroy fun shutdown() {
        closed = true; running = false; consumer?.wakeup(); inboxConsumer?.wakeup()
        worker?.join(20000)
    }
}

data class NotificationCommand(val action: String, val orderId: String? = null)
