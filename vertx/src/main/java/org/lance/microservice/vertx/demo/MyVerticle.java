package org.lance.microservice.vertx.demo;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;

/**
 * 自定义的Verticle
 *
 * Asynchronous Verticle start and stop
 *
 * @author Lance
 */
public class MyVerticle extends AbstractVerticle {

    private HttpServer server;

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        server = vertx.createHttpServer().requestHandler(req -> req.response()
                .putHeader("content-type", "text/plain")
                .end("Hello from Vert.x!"));

        // Now bind the server:
        server.listen(8080, res -> {
            if (res.succeeded()) {
                startPromise.complete();
            } else {
                startPromise.fail(res.cause());
            }
        });
    }

    /**
     * Vert.x will automatically stop any running server when the verticle is undeployed
     *
     * @param stopPromise
     * @throws Exception
     */
    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {
        // do something that takes time
        server.close(res -> {
            if (res.succeeded()) {
                stopPromise.complete();
            } else {
                stopPromise.fail(res.cause());
            }
        });
    }
}
