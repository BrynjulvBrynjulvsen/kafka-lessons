package io.bekk.kafkalessons

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException

@RestController
@RequestMapping("/api/notifications")
class NotificationController(private val runtime: NotificationRuntime) {
    @GetMapping fun state() = runtime.snapshot()
    @PostMapping fun command(@RequestBody command: NotificationCommand) = runtime.command(command)
    @ExceptionHandler(ResponseStatusException::class)
    fun failure(error: ResponseStatusException): ResponseEntity<Map<String, String>> =
        ResponseEntity.status(error.statusCode).body(mapOf("message" to (error.reason ?: "Notification command rejected")))
}
