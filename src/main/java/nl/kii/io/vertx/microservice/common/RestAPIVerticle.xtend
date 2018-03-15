package nl.kii.io.vertx.microservice.common

import io.vertx.core.Future
import io.vertx.core.http.HttpHeaders
import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.jwt.JWTAuth
import io.vertx.ext.dropwizard.MetricsService
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.CorsHandler
import io.vertx.ext.web.handler.JWTAuthHandler
import java.util.Set

import static io.vertx.core.http.HttpMethod.*

/** 
 * An abstract base verticle that provides several helper methods for REST API.
 */
abstract class RestAPIVerticle extends BaseMicroserviceVerticle {

    /** 
     * Create http server for the REST service.
     * @param router router instance
     * @param host   http host
     * @param port   http port
     * @return async result of the procedure
     */
    def protected Future<Void> createHttpServer(extension Router router, String host, int port) {
        val httpServerFuture = Future.future
        vertx.createHttpServer
            .requestHandler [accept]
            .listen(port, host, httpServerFuture.completer)
        return httpServerFuture.map [null]
    }

    /** 
     * Enable CORS support.
     * @param router router instance
     */
    def protected void enableCorsSupport(extension Router router) {
        val cors = CorsHandler.create('*')
            .allowedHeaders(#{'Content-Type', 'Authorization', 'X-Requested-With', 'Content-Length', 'Accept', 'Origin'})
            .allowedMethods(#{GET, POST, PUT, DELETE, HEAD, OPTIONS})
            .allowCredentials(true)        
        route.handler(cors)
    }

    /** 
     * Enable simple heartbeat check mechanism via HTTP.
     * @param router router instance
     * @param config configuration object
     */
    def protected void enableHeartbeatCheck(extension Router router, JsonObject config) {
        get(config.getString('heartbeat.path', '/health'))
            .produces('application/json')
            .handler [context| 
                var checkResult = new JsonObject().put('status', 'UP')
                context.ok(checkResult)
            ]
    }
    
    /** 
     * Enable simple metrics mechanism via HTTP.
     * @param router router instance
     * @param config configuration object
     */
    def protected void enableMetrics(extension Router router, JsonObject config) {
        val metrics = MetricsService.create(vertx)
        get(config.getString('metrics.path', '/metrics'))
            .produces('application/json')
            .handler [context|
                val snapshot = metrics.getMetricsSnapshot(vertx)
                context.ok(snapshot)
            ]
    }
    
    /** 
     * Enable JWT Authentication mechanism via HTTP.
     * @param router router instance
     * @param config configuration object
     * @param authorities the authorities must have
     */
    def protected void enableJWTAuth(extension Router router, JsonObject config, Set<String> authorities) {
        val handler = JWTAuthHandler.create(JWTAuth.create(vertx, config))
        if (!authorities.empty) handler.addAuthorities(authorities)
        route.handler(handler)
    }

    // HTTP reponses helper
    
    // 200 OK
    def protected void ok(RoutingContext context, JsonObject json) {
        context.response
            .putHeader(HttpHeaders.CONTENT_TYPE, 'application/json')
            .end(json.encodePrettily)
    }
    
    // 201 CREATED
    def protected void created(RoutingContext context, String location) {
        context.response
            .putHeader(HttpHeaders.LOCATION, location)
            .setStatusCode(201)
            .end
    }

    // 202 ACCEPTED
    def protected void accepted(RoutingContext context, String location) {
        context.response
            .setStatusCode(202)
            .putHeader(HttpHeaders.LOCATION, location)
            .end
    }
    
    // 204 NO CONTENT
    def protected void noContent(RoutingContext context) {
        context.response
            .setStatusCode(204)
            .end
    }

    // 301 MOVED PERMANENTELY
    
    // 302 FOUND
    
    // 303 SEE OTHER
    def protected void seeOther(RoutingContext context, String location) {
        context.response
            .putHeader(HttpHeaders.LOCATION, location)
            .setStatusCode(303)
            .end
    }
    
    // 304 NOT MODIFIED

    // 400 BAD REQUEST
    def protected void badRequest(RoutingContext context, Throwable ex) {
        val json = new JsonObject => [
            put('error', 'BAD_REQUEST')
            put('message', ex.message)
        ]
        context.response
            .putHeader(HttpHeaders.CONTENT_TYPE, 'application/json')
            .setStatusCode(400)
            .end(json.encodePrettily)
    }

    // 401 UNAUTHORIZED
    
    // 401 FORBIDDEN

    // 404 NOT FOUND
    def protected void notFound(RoutingContext context) {
        val json = new JsonObject => [
            put('error', 'NOT_FOUND')            
        ]
        context.response
            .putHeader(HttpHeaders.CONTENT_TYPE, 'application/json')
            .setStatusCode(404)
            .end(json.encodePrettily)
    }
    
    // 405 METHOD NOT ALLOWED
    
    // 410 GONE
    
    // 415 UNSUPPORTED MEDIA TYPE

    // 429 TOO MANY REQUESTS

    // 500 INTERNAL SERVER ERROR
    def protected void internalError(RoutingContext context, Throwable ex) {
        val json = new JsonObject => [
            put('error', 'INTERNAL_ERROR')
            put('message', ex.message)
        ]
        
        context.response
            .putHeader(HttpHeaders.CONTENT_TYPE, 'application/json')
            .setStatusCode(500)
            .end(json.encodePrettily)
    }

    // 501 NOT IMPLEMENTED
    def protected void notImplemented(RoutingContext context) {
        val json = new JsonObject => [
            put('error', 'NOT_IMPLEMENTED')
        ]
        
        context.response
            .putHeader(HttpHeaders.CONTENT_TYPE, 'application/json')
            .setStatusCode(501)
            .end(json.encodePrettily)
    }

    // 502 BAD GATEWAY
    def protected void badGateway(RoutingContext context, Throwable ex) {
        val json = new JsonObject => [
            put('error', 'BAD_GATEWAY')
            put('message', ex.message)
        ]        
        
        ex.printStackTrace
        context.response
            .putHeader(HttpHeaders.CONTENT_TYPE, 'application/json')
            .setStatusCode(502)
            .end(json.encodePrettily)
    }

    def protected void serviceUnavailable(RoutingContext context) {
        context.fail(503)
    }

    def protected void serviceUnavailable(RoutingContext context, Throwable ex) {
        serviceUnavailable(context, ex.message)
    }

    // 503 SERVICE UNAVAILABLE
    def protected void serviceUnavailable(RoutingContext context, String cause) {
        val json = new JsonObject => [
            put('error', 'SERVICE_UNAVAILABLE')
            put('message', cause)
        ]        
        
        context.response
            .putHeader(HttpHeaders.CONTENT_TYPE, 'application/json')
            .setStatusCode(503)
            .end(json.encodePrettily)
    }

}
