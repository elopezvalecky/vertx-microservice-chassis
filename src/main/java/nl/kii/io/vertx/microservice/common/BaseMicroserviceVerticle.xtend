package nl.kii.io.vertx.microservice.common

import io.vertx.circuitbreaker.CircuitBreaker
import io.vertx.circuitbreaker.CircuitBreakerOptions
import io.vertx.core.AbstractVerticle
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.impl.ConcurrentHashSet
import io.vertx.core.json.JsonObject
import io.vertx.servicediscovery.Record
import io.vertx.servicediscovery.ServiceDiscovery
import io.vertx.servicediscovery.ServiceDiscoveryOptions
import io.vertx.servicediscovery.types.EventBusService
import io.vertx.servicediscovery.types.HttpEndpoint
import io.vertx.servicediscovery.types.JDBCDataSource
import io.vertx.servicediscovery.types.MessageSource
import java.util.Set

import static extension io.vertx.core.logging.LoggerFactory.getLogger

/**
 * This verticle provides support for various microservice functionality
 * like service discovery, circuit breaker and simple log publisher.
 */
abstract class BaseMicroserviceVerticle extends AbstractVerticle {

    val logger = class.logger

    protected ServiceDiscovery discovery;
    protected CircuitBreaker circuitBreaker;
    protected Set<Record> registeredRecords = new ConcurrentHashSet<Record>

    override start() throws Exception {
        super.start

        // Initialize service discovery
        val discoveryOptions = new ServiceDiscoveryOptions => [
            backendConfiguration = config
        ]
        discovery = ServiceDiscovery.create(vertx, discoveryOptions)

        // Initialize circuit breaker
        val breakerConfig = config.getJsonObject('circuit-breaker') ?: new JsonObject
        val breakerOptions = new CircuitBreakerOptions => [
            maxFailures = breakerConfig.getInteger('max-failures', 5)
            timeout = breakerConfig.getLong('timeout', 10000L)
            fallbackOnFailure = true
            resetTimeout = breakerConfig.getLong('reset-timeout', 30000L)
        ]
        circuitBreaker = CircuitBreaker.create(breakerConfig.getString('name', 'circuit-breaker'), vertx, breakerOptions)
    }

    override stop(Future<Void> future) throws Exception {
        stop
        val futures = newArrayList
        registeredRecords.stream.forEach [
            val unregistrationFuture = Future.<Void>future
            futures.add(unregistrationFuture)
            discovery.unpublish(registration, unregistrationFuture.completer)
            logger.info('''Service <«name»> unpublished''');
        ]

        if (futures.empty) {
            discovery.close
            future.complete()
        } else {
            CompositeFuture.all(futures).setHandler [
                discovery.close
                if (failed) {
                    future.fail(cause)
                } else {
                    future.complete()
                }
            ]
        }
    }

    def protected Future<Void> publishHttpEndpoint(String name, String host, int port) {
        val metadata = new JsonObject => [
            put('api.name', config.getString('api.name', ''))
        ]
        HttpEndpoint.createRecord(name, host, port, '/', metadata).publish
    }

    def protected Future<Void> publishMessageSource(String name, String address) {
        MessageSource.createRecord(name, address).publish
    }

    def protected Future<Void> publishJDBCDataSource(String name, JsonObject location) {
        JDBCDataSource.createRecord(name, location, new JsonObject).publish
    }

    def protected <T> Future<Void> publishEventBusService(String name, String address, Class<T> serviceClass) {
        EventBusService.createRecord(name, address, serviceClass).publish
    }

    /**
     * Publish a service with record.
     * 
     * @param record service record
     * @return async result
     */
    def private Future<Void> publish(Record record) {
        if (discovery === null) {
            try {
                this.start
            } catch (Throwable t) {
                throw new IllegalStateException('Cannot create discovery service', t)
            }
        }

        val future = Future.<Void>future
        discovery.publish(record) [
            if (succeeded) {
                registeredRecords.add(record);
                logger.info('''Service <«result.name»> published''');
                future.complete();
            } else {
                future.fail(cause)
            }
        ]
        future
    }

}