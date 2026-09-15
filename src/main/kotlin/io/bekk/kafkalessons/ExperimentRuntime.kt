package io.bekk.kafkalessons

import io.bekk.kafkademo.core.*

import jakarta.annotation.PreDestroy
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/** Teaching workers are independent of the application's observation consumer. */
@Component
class ExperimentRuntime(
    factory: ConsumerFactory<String, String>,
    private val producer: KafkaTemplate<String, String>,
    private val streams: TopicWebSocketHandler,
    private val demo: DemoProperties,
    @param:Value("\${demo.experiment.enabled:true}") val enabled: Boolean,
    @param:Value("\${demo.experiment.topic:kafka-demo-lab}") val topic: String,
    @Value("\${demo.experiment.group-prefix:kafka-demo-experiment}") prefix: String,
) {
    val groups = listOf("$prefix-a", "$prefix-b")
    private val properties = factory.configurationProperties.toMutableMap()
    private val admin by lazy { Admin.create(properties) }
    private var adminUsed = false
    private val workers = linkedMapOf<String, Worker>()
    private val events = ArrayDeque<Map<String, Any?>>()
    private var serial = 0L
    private var partitions = emptyList<TopicPartition>()
    private var offsets: List<Map<String, Any?>> = emptyList()
    private var sampledAt: Long? = null
    private var sampleError: String? = null
    private val scheduler = Executors.newSingleThreadScheduledExecutor { r -> Thread(r, "experiment-sampler").apply { isDaemon = true } }
    private val groupDelays = java.util.concurrent.ConcurrentHashMap<String, Long>().apply {
        groups.forEach { put(it, 0L) }
    }
    @Volatile private var producing = false
    @Volatile private var stopProduction = false
    @Volatile private var closed = false
    private var workload: Thread? = null

    init {
        if (enabled) {
            require(topic in demo.topics) { "Experiment topic must be in demo.topics" }
            require(groups.none { it == properties[ConsumerConfig.GROUP_ID_CONFIG] }) { "Experiment groups must differ from observer group" }
            scheduler.scheduleWithFixedDelay({ sample() }, 0, 1, TimeUnit.SECONDS)
        }
    }

    @Synchronized fun snapshot(): Map<String, Any?> = mapOf(
        "type" to "experiment-snapshot", "version" to 1, "topic" to topic,
        "enabled" to enabled, "groups" to groups, "at" to System.currentTimeMillis(),
        "partitions" to partitions.map { it.partition() }, "members" to workers.values.map { it.view() },
        "offsets" to offsets, "sampledAt" to sampledAt, "error" to sampleError,
        "events" to events.toList(), "groupDelays" to groupDelays.toMap(), "producing" to producing,
    )

    @Synchronized private fun event(kind: String, group: String? = null, member: String? = null,
                                    partition: Int? = null, offset: Long? = null, detail: String? = null) {
        events.addLast(mapOf("id" to ++serial, "kind" to kind, "group" to group, "member" to member,
            "partition" to partition, "offset" to offset, "detail" to detail, "at" to System.currentTimeMillis()))
        while (events.size > 48) events.removeFirst()
    }

    private fun checkEnabled() {
        if (!enabled || closed) throw ResponseStatusException(HttpStatus.CONFLICT, "Experiment runtime is disabled")
    }
    private fun checkGroup(group: String?) {
        if (group !in groups) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown experiment group")
    }

    // Only commands change membership. Never called by subscribe or browser navigation.
    @Synchronized fun command(request: ExperimentCommand): Map<String, Any?> {
        checkEnabled()
        when (request.action) {
            "start" -> {
                checkGroup(request.group)
                requireInput(request.policy in listOf("earliest", "latest"), "Choose earliest or latest")
                requireInput(workers.values.count { it.group == request.group && it.thread.isAlive } < 4, "At most four members per group")
                val worker = Worker(request.group!!, request.policy)
                workers.entries.removeIf { !it.value.thread.isAlive }
                workers[worker.id] = worker
                worker.thread.start()
            }
            "stop" -> {
                checkGroup(request.group)
                workers.values.filter { it.group == request.group }.forEach { it.stop() }
            }
            "delay" -> {
                checkGroup(request.group)
                requireInput(request.delayMs in 0..1000, "Processing delay must be 0–1000 ms")
                groupDelays[request.group!!] = request.delayMs
            }
            "produce" -> {
                requireInput(!producing, "A workload is already running")
                requireInput(request.count in 1..120 && request.intervalMs in 20..1000, "Use 1–120 records and 20–1000 ms spacing")
                requireInput(partitions.isNotEmpty() && sampleError == null, "Wait for a successful broker sample")
                producing = true
                stopProduction = false
                val targets = partitions.toList()
                val run = UUID.randomUUID().toString().take(8)
                workload = Thread({
                    try {
                        for (index in 0 until request.count) {
                            if (stopProduction || closed) break
                            // Explicit partitions provide balanced input for the group/lag experiment.
                            val partition = targets[index % targets.size].partition()
                            producer.send(topic, partition, "lane-$partition", "$run / ${index + 1}").get(10, TimeUnit.SECONDS)
                            Thread.sleep(request.intervalMs)
                        }
                        event("workload-stopped", detail = run)
                    } catch (_: InterruptedException) {
                        Thread.currentThread().interrupt()
                    } catch (e: Exception) { event("workload-error", detail = "${e.message}; last write may have succeeded") }
                    finally { producing = false }
                }, "experiment-producer").apply { isDaemon = true; start() }
            }
            "stop-production" -> { stopProduction = true }
            "reset" -> {
                checkGroup(request.group)
                requireInput(workers.values.none { it.group == request.group && it.thread.isAlive }, "Stop all group members and wait for them to exit")
                adminUsed = true
                val description = admin.describeConsumerGroups(listOf(request.group!!)).all().get(5, TimeUnit.SECONDS)[request.group]!!
                requireInput(description.members().isEmpty(), "Group still has active members; wait for it to become empty")
                val selected = partitions.singleOrNull { it.partition() == request.partition }
                    ?: throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown partition")
                val begin = admin.listOffsets(mapOf(selected to OffsetSpec.earliest())).all().get(5, TimeUnit.SECONDS)[selected]!!.offset()
                val end = admin.listOffsets(mapOf(selected to OffsetSpec.latest())).all().get(5, TimeUnit.SECONDS)[selected]!!.offset()
                requireInput(request.offset in begin..end, "Offset must be within retained range $begin–$end")
                admin.alterConsumerGroupOffsets(request.group, mapOf(selected to OffsetAndMetadata(request.offset))).all().get(5, TimeUnit.SECONDS)
                event("offset-reset", request.group, partition = request.partition, offset = request.offset)
            }
            else -> throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown action")
        }
        return snapshot()
    }

    private fun requireInput(valid: Boolean, message: String) {
        if (!valid) throw ResponseStatusException(HttpStatus.CONFLICT, message)
    }

    private fun sample() {
        try {
            adminUsed = true
            val inventory = admin.describeTopics(listOf(topic)).allTopicNames().get(5, TimeUnit.SECONDS)[topic]!!
                .partitions().map { TopicPartition(topic, it.partition()) }
            val ends = admin.listOffsets(inventory.associateWith { OffsetSpec.latest() }).all().get(5, TimeUnit.SECONDS)
            val starts = admin.listOffsets(inventory.associateWith { OffsetSpec.earliest() }).all().get(5, TimeUnit.SECONDS)
            val rows = groups.flatMap { group ->
                val committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)
                inventory.map { tp ->
                    val commit = committed[tp]?.offset()
                    val end = ends[tp]!!.offset()
                    mapOf("group" to group, "partition" to tp.partition(), "start" to starts[tp]!!.offset(),
                        "end" to end, "committed" to commit, "lag" to commit?.let { end - it })
                }
            }
            synchronized(this) { partitions = inventory; offsets = rows; sampledAt = System.currentTimeMillis(); sampleError = null }
        } catch (e: Exception) { synchronized(this) { sampleError = "Broker sample unavailable: ${e.message}" } }
        streams.publishSnapshot(topic, snapshot())
    }

    private inner class Worker(val group: String, policy: String) {
        val id = UUID.randomUUID().toString().take(8)
        @Volatile private var running = true
        @Volatile private var consumer: KafkaConsumer<String, String>? = null
        private var state = "joining"
        private var assignment = emptyList<Int>()
        private val position = mutableMapOf<Int, Long>()
        private val processed = mutableMapOf<Int, Long>()
        private val committed = mutableMapOf<Int, Long>()
        val thread = Thread({ run(policy) }, "experiment-$id").apply { isDaemon = true }
        fun view() = mapOf("id" to id, "group" to group, "state" to state, "assignment" to assignment,
            "position" to position.toMap(), "processed" to processed.toMap(), "committed" to committed.toMap())
        fun stop() {
            if (!thread.isAlive) return
            running = false; state = "stopping"; consumer?.wakeup()
        }

        private fun run(policy: String) {
            try {
                val config = properties.toMutableMap().apply {
                    remove(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG)
                    put(ConsumerConfig.GROUP_ID_CONFIG, group)
                    put(ConsumerConfig.CLIENT_ID_CONFIG, "experiment-$id")
                    put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                    put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, policy)
                    put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 6)
                    put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000)
                    put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false)
                }
                KafkaConsumer<String, String>(config).use { kafka ->
                    consumer = kafka
                    kafka.subscribe(listOf(topic), object : ConsumerRebalanceListener {
                        override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
                            synchronized(this@ExperimentRuntime) {
                                assignment = emptyList(); position.clear(); processed.clear(); committed.clear(); state = "rebalancing"
                                event("revoked", group, id, detail = partitions.joinToString())
                            }
                        }
                        override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                            synchronized(this@ExperimentRuntime) {
                                assignment = partitions.map { it.partition() }.sorted(); state = "running"
                                event("assigned", group, id, detail = assignment.joinToString())
                            }
                        }
                    })
                    while (running && !closed) {
                        val records = kafka.poll(Duration.ofMillis(250))
                        val positions = kafka.assignment().associate { it.partition() to kafka.position(it) }
                        synchronized(this@ExperimentRuntime) { position.putAll(positions) }
                        val completed = mutableMapOf<TopicPartition, OffsetAndMetadata>()
                        for (record in records) {
                            if (!running || closed) break
                            event("delivered", group, id, record.partition(), record.offset(), record.value()?.take(80))
                            Thread.sleep(groupDelays.getValue(group))
                            // This explicit delay is the whole demo processing step, not a business effect.
                            synchronized(this@ExperimentRuntime) { processed[record.partition()] = record.offset() + 1 }
                            event("processed", group, id, record.partition(), record.offset())
                            completed[TopicPartition(topic, record.partition())] = OffsetAndMetadata(record.offset() + 1)
                        }
                        if (completed.isNotEmpty() && running && !closed) {
                            kafka.commitSync(completed, Duration.ofSeconds(5))
                            synchronized(this@ExperimentRuntime) {
                                completed.forEach { (tp, next) ->
                                    committed[tp.partition()] = next.offset()
                                    event("committed", group, id, tp.partition(), next.offset())
                                }
                            }
                        }
                    }
                }
            } catch (_: WakeupException) {
                if (running && !closed) event("worker-error", group, id, detail = "Unexpected wakeup")
            } catch (e: Exception) { event("worker-error", group, id, detail = e.message) }
            finally {
                consumer = null
                synchronized(this@ExperimentRuntime) { state = "stopped"; assignment = emptyList() }
                event("stopped", group, id)
            }
        }
    }

    @PreDestroy fun shutdown() {
        closed = true; producing = false
        scheduler.shutdownNow()
        val active = synchronized(this) { workers.values.toList().also { list -> list.forEach { it.stop() } } }
        workload?.interrupt()
        active.forEach { it.thread.join(7000) }
        if (adminUsed) admin.close(Duration.ofSeconds(2))
    }
}

data class ExperimentCommand(
    val action: String, val group: String? = null, val policy: String = "earliest",
    val delayMs: Long = 0, val count: Int = 60, val intervalMs: Long = 50,
    val partition: Int = 0, val offset: Long = 0,
)
