package io.bekk.kafkalessons

import io.bekk.kafkademo.core.*

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException

@RestController
@RequestMapping("/api/experiment")
class ExperimentController(private val runtime: ExperimentRuntime) {
    @ExceptionHandler(ResponseStatusException::class)
    fun failure(error: ResponseStatusException): ResponseEntity<Map<String, String>> =
        ResponseEntity.status(error.statusCode).body(mapOf("message" to (error.reason ?: "Experiment command failed")))
    @GetMapping fun state() = runtime.snapshot()
    @PostMapping fun command(@RequestBody request: ExperimentCommand): Map<String, Any?> = try {
        runtime.command(request)
    } catch (error: ResponseStatusException) { throw error }
    catch (error: Exception) { throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
        "Experiment command failed; inspect current state before retrying: ${error.message}") }
}
