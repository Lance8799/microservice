package org.lance.microservice.vertx.demo;

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.*;
import io.vertx.core.eventbus.impl.codecs.StringMessageCodec;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.pointer.JsonPointer;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.NetServer;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * 核心API
 *
 * 黄金法则 - Don’t Block the Event Loop
 *
 * 阻塞包含：
 *  Thread.sleep()
 *  Waiting on a lock
 *  Waiting on a mutex or monitor (e.g. synchronized section)
 *  Doing a long lived database operation and waiting for a result
 *  Doing a complex calculation that takes some significant time.
 *  Spinning in a loop
 *
 *
 * @author Lance
 */
public class CoreDemo {

    private Vertx vertx = Vertx.vertx();

    // Specifying options to creating Vertx
//        Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(40));

    /**
     * 运行阻塞方法
     */
    public void runBlocking(){
        WorkerExecutor executor = vertx.createSharedWorkerExecutor("my-worker-pool", 2, 5, TimeUnit.SECONDS);
        executor.executeBlocking(promise -> {
            // Call some blocking API that takes a significant amount of time to return
            String result = blockingMethod("hello");
            promise.complete(result);
        }, res -> System.out.println("The result is: " + res.result()));

        // executor must be closed when it’s not necessary
        executor.close();
    }

    /**
     * 异步协作
     */
    public void asyncCoordination() {

        Future<HttpServer> httpServerFuture = Future.future(promise -> vertx.createHttpServer().listen(promise));
        Future<NetServer> netServerFuture = Future.future(promise -> vertx.createNetServer().listen(promise));
        Future<String> succeededFuture = Future.succeededFuture("succeed");

        // all the futures are succeeded
        CompositeFuture.all(httpServerFuture, netServerFuture).setHandler(ar -> {
            if (ar.succeeded()) {
                // All servers started
            } else {
                // At least one server failed
            }
        });
        CompositeFuture.all(Arrays.asList(httpServerFuture, netServerFuture, succeededFuture));

        // first succeeded future
        CompositeFuture.any(httpServerFuture, netServerFuture).setHandler(ar -> {
            if (ar.succeeded()) {
                // At least one is succeeded
            } else {
                // All failed
            }
        });

        // waits until all futures are completed, either with a success or a failure
        CompositeFuture.join(httpServerFuture, netServerFuture, succeededFuture).setHandler(ar -> {
            if (ar.succeeded()) {
                // All succeeded
            } else {
                // All completed and at least one failed
            }
        });
    }

    /**
     * 异步顺序组合
     */
    public void sequentialComposition() {
        FileSystem fileSystem = vertx.fileSystem();

        Future<Void> fileFuture = Future.future(promise -> fileSystem.createFile("/foo", promise));
        // if one of the steps fails, the final future is failed.
        fileFuture
                // When the file is created (fut1), execute this:
                .compose(v -> Future.<Void>future(promise -> fileSystem.writeFile("/foo", Buffer.buffer(), promise)))
                // When the file is written (fut2), execute this:
                .compose(v -> Future.<Void>future(promise -> fileSystem.move("/foo", "/bar", promise)));
    }

    /**
     * 发布verticle
     *
     */
    public void deployingVerticles() {
        DeploymentOptions options = new DeploymentOptions()
                .setWorker(true)
                // matches the worker pool size below
                .setInstances(5)
                .setWorkerPoolName("the-specific-pool")
                .setWorkerPoolSize(5);

        vertx.deployVerticle(new MyVerticle(), options, rs -> {
            if (rs.succeeded()) {
                System.out.println("Deploy succeed. Id is: " + rs.result());
            } else {
                System.out.println("Deploy failed.");
            }
        });

        // Deploy a Java verticle
        vertx.deployVerticle("com.mycompany.MyOrderProcessorVerticle");
        // Deploy a JavaScript verticle
        vertx.deployVerticle("verticles/myverticle.js");
        // Deploy a Ruby verticle verticle
        vertx.deployVerticle("verticles/my_verticle.rb");

        // Undeploy verticle
        vertx.undeploy("deploymentID", res -> {
            if (res.succeeded()) {
                System.out.println("Undeployed ok");
            } else {
                System.out.println("Undeploy failed!");
            }
        });

        // Deploy with config
        JsonObject config = new JsonObject().put("name", "tim").put("directory", "/blah");
        DeploymentOptions configOptions = new DeploymentOptions().setConfig(config);
        vertx.deployVerticle("com.mycompany.MyOrderProcessorVerticle", configOptions);

        // Isolation Deploy. Only the classes that match will be isolated - any other classes will be loaded by the current class loader.
        DeploymentOptions isolationOptions = new DeploymentOptions().setIsolationGroup("mygroup");
        options.setIsolatedClasses(
                Arrays.asList("com.mycompany.myverticle.*", "com.mycompany.somepkg.SomeClass", "org.somelibrary.*"));
        vertx.deployVerticle("com.mycompany.myverticle.VerticleClass", isolationOptions);
    }

    /**
     * 运行context对象
     *
     */
    public void context() {
        Context context = vertx.getOrCreateContext();

        if (context.isEventLoopContext()) {
            System.out.println("Context attached to Event Loop");
        } else if (context.isWorkerContext()) {
            System.out.println("Context attached to Worker Thread");
        } else if (context.isMultiThreadedWorkerContext()) {
            System.out.println("Context attached to Worker Thread - multi threaded worker");
        } else if (! Context.isOnVertxThread()) {
            System.out.println("Context not attached to a thread managed by vert.x");
        }

        // run code in this context asynchronously
        context.runOnContext( (v) -> System.out.println("This will be executed asynchronously in the same context"));

        // When several handlers run in the same context, they may want to share data.
        context.put("data", "hello");
        context.runOnContext((v) -> {
            String hello = context.get("data");
            System.out.println(hello);
        });
    }

    /**
     * 执行周期和延时操作
     */
    public void periodicAndDelayed() {
        // One-shot Timers
        long timerID = vertx.setTimer(1000, id -> System.out.println("And one second later this is printed"));
        System.out.println("First this is printed");

        // Periodic Timers
        long periodicTimerID = vertx.setPeriodic(1000, id -> System.out.println("And every second this is printed"));
        System.out.println("First this is printed");

        // Cancelling timers
        vertx.cancelTimer(timerID);
    }

    /**
     * 事件总线API
     */
    public void eventBus() {
        EventBus eventBus = vertx.eventBus();
        String address = "news.sport";

        MessageConsumer<Object> consumer = eventBus.consumer(address);
        consumer.handler(message -> System.out.println(message.body()));

        // When registering a handler on a clustered event bus
        consumer.completionHandler(result -> {
            if (result.succeeded()) {
                System.out.println("register complete");
            } else  {
                System.out.println("register failed");
            }
        });

        consumer.unregister(result -> {
            if (result.succeeded()) {
                System.out.println("unregister succeed");
            }
        });

        // Publishing messages, That message will then be delivered to all handlers registered
        eventBus.publish(address, "This is all news.");

        // Sending message, This is the point-to-point messaging pattern. The handler is chosen in a non-strict round-robin.
        eventBus.send(address, "This is a news.");

        // headers on messages
        DeliveryOptions options = new DeliveryOptions().addHeader("header", "value");
        eventBus.send(address, "This is a news.", options);

        // Acknowledging messages, received the message and "processed" it using request-response pattern.
        // The receiver
        eventBus.consumer(address, message -> {
            System.out.println("Received a message. " + message.body());
            message.reply("Got it");
        });
        //The sender
        eventBus.request(address, "This is a request new.", re -> {
            if (re.succeeded()) {
                System.out.println("Received ack: " + re.result().body());
            }
        });

        // Sending default timeout is 30 seconds.

        // Message Codecs, send any object you like across the event bus
        MessageCodec messageCodec = new StringMessageCodec();
        eventBus.registerCodec(messageCodec);

        // can write a codec that allows a MyPOJO class to be sent,
        // but when that message is sent to a handler it arrives as a MyOtherPOJO class.
        eventBus.send(address, new POJO(), new DeliveryOptions().setCodecName(messageCodec.name()));

        // always want the same codec
        eventBus.registerDefaultCodec(POJO.class, messageCodec);
        eventBus.send(address, new POJO());

        // Configuring the event bus
        VertxOptions clusterOptions = new VertxOptions().setEventBusOptions(
                new EventBusOptions()
                        .setClusterPublicHost("hostname")
                        .setClusterPublicPort(9999)
                        .setSsl(true)
                        // jks(java key store)
                        .setKeyStoreOptions(new JksOptions().setPath("keystore.jks").setPassword("123456"))
                        .setTrustStoreOptions(new JksOptions().setPath("keystore.jks").setPassword("123456"))
                        .setClientAuth(ClientAuth.REQUIRED)
                );

        // Clustered Event Bus, must configure the cluster manager to use encryption or enforce security
        Vertx.clusteredVertx(clusterOptions, event -> {
            if (event.succeeded()) {
                Vertx vertx = event.result();
                EventBus clusterEventBus = vertx.eventBus();
                System.out.println("Now have a clustered event bus:" + clusterEventBus);
            }
        });
    }

    /**
     * json操作
     */
    public void json() {
        JsonObject jsonObject = new JsonObject();
        jsonObject.put("foo", "bar").put("num", 123).put("bool", true);

        vertx.createHttpClient().getNow(8080, "localhost", "/", resp -> resp.bodyHandler(buff -> {
            JsonObject json = buff.toJsonObject();
            POJO javaObject = json.mapTo(POJO.class);
            System.out.println(javaObject);
        }));

        JsonArray array = new JsonArray();
        array.add("foo").add(123).add(false);

        // JSON pointer
        Object newJsonObject = JsonPointer.from("/foo").writeJson(jsonObject, "new"); // {"foo":"new","num":123,"bool":true}
        JsonPointer.from("/num").queryJson(newJsonObject); // new
    }

    /**
     * 阻塞方法
     *
     * @param arg
     * @return
     */
    private String blockingMethod(String arg) {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return arg + " - blocking";
    }

    class POJO {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static void main(String[] args) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.put("foo", "bar").put("num", 123).put("bool", true);

        Object newJsonObject = JsonPointer.from("/num").writeJson(jsonObject, "new");

        Object queryJson = JsonPointer.from("/num").queryJson(newJsonObject);
        System.out.println(queryJson);
    }
}
