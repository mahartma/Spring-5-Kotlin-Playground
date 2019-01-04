package de.codecentric.workshop.spring5

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.springframework.amqp.core.Queue
import org.springframework.amqp.rabbit.connection.ConnectionFactory
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.integration.amqp.dsl.Amqp
import org.springframework.integration.channel.PublishSubscribeChannel
import org.springframework.integration.dsl.IntegrationFlow
import org.springframework.integration.dsl.IntegrationFlows
import org.springframework.messaging.Message
import org.springframework.messaging.MessageHandler
import org.springframework.messaging.SubscribableChannel
import org.springframework.web.reactive.HandlerMapping
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.WebSocketSession
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink
import java.util.function.Consumer

@SpringBootApplication
class WebApp {

    @Bean
    fun inboundChannel() = PublishSubscribeChannel()

    @Bean
    fun rabbitFlow(connectionFactory: ConnectionFactory, inboundChannel: SubscribableChannel): IntegrationFlow =
            IntegrationFlows
                    .from(Amqp.inboundAdapter(connectionFactory, Queue("test_queue")))
                    .channel(inboundChannel)
                    .get()

    @Bean
    fun webSocketHandlerMapping(): HandlerMapping {
        val map: HashMap<String, WebSocketHandler> = hashMapOf("/event-emitter" to webSocketHandler())
        val handlerMapping = SimpleUrlHandlerMapping()
        handlerMapping.order = 1
        handlerMapping.urlMap = map
        return handlerMapping
    }

    fun webSocketHandler(): WebSocketHandler {
        val connections = HashMap<String, MessageHandler>()

        return WebSocketHandler { session: WebSocketSession ->
            val rabbit2webSocketFlux = Flux.create(Consumer<FluxSink<WebSocketMessage>> {
                val messageHandler = WebsocketMessageHandler(session, it)
                connections[session.id] = messageHandler
                inboundChannel().subscribe(messageHandler)
            }).doFinally {
                connections[session.id]?.apply {
                    inboundChannel().unsubscribe(this)
                    connections.remove(session.id)
                }
            }
            session
                    .send(rabbit2webSocketFlux)
                    .and(session.receive())
        }
    }

    @Bean
    fun webSocketHandlerAdapter() = WebSocketHandlerAdapter()
}


fun main(args: Array<String>) {
    SpringApplication.run(WebApp::class.java, *args)
}

class WebsocketMessageHandler(private val session: WebSocketSession, private val sink: FluxSink<WebSocketMessage>) : MessageHandler {
    private val mapper = ObjectMapper()

    init {
        mapper.registerKotlinModule()
    }

    override fun handleMessage(msg: Message<*>) {
        val event = mapper.readValue(msg.payload as ByteArray, Event::class.java)
        sink.next(session.textMessage(mapper.writeValueAsString(event)))
    }
}

data class Event(val test: String)
