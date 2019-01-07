package de.codecentric.workshop.spring5

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.hamcrest.Matchers.`is`
import org.junit.Assert.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT
import org.springframework.boot.web.server.LocalServerPort
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.web.reactive.socket.WebSocketSession
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient
import org.springframework.web.reactive.socket.client.WebSocketClient
import reactor.core.publisher.Flux
import reactor.core.publisher.MonoProcessor
import java.net.URI
import java.time.Duration

@RunWith(SpringRunner::class)
@SpringBootTest(webEnvironment = RANDOM_PORT)
class WebAppTest {

    @LocalServerPort
    var port: Int? = null

    final var mapper = ObjectMapper()

    @Autowired
    lateinit var client: WebSocketClient

    @Autowired
    lateinit var rabbbitTemplate: RabbitTemplate

    init {
        mapper.registerKotlinModule()
    }

    @Test
    fun `sollte Event per Websocket empfangen`() {
        val output: MonoProcessor<String> = MonoProcessor.create()

        client.execute(URI.create("ws://localhost:$port/event-emitter"))
        { session: WebSocketSession ->

            rabbbitTemplate.convertAndSend("amq.topic", "test.queue",
                    mapper.writeValueAsBytes(Event("max")))

            session.send(Flux.empty())
                    .and(session.receive()
                            .map { it.payloadAsText }
                            .subscribeWith(output)
                            .then())
        }.block(TIMEOUT)

        assertThat(mapper.readValue(output.block(TIMEOUT), Event::class.java), `is`(Event("max")))
    }

}

@Configuration
class TestConfig {
    @Bean
    fun client() = ReactorNettyWebSocketClient()
}

private val TIMEOUT = Duration.ofMillis(5000)