/*
 * Copyright (c) 2017. Hans-Peter Grahsl (grahslhp@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package at.grahsl.kafka.connect.mongodb.end2end;

import at.grahsl.kafka.connect.mongodb.data.avro.TweetMsg;
import okhttp3.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

@RunWith(JUnitPlatform.class)
public class ContainerTest {

    public static String COMPOSE_FILE = "src/test/resources/docker/compose-env.yml";

    public static String KAFKA_BROKER_SERVICE = "kafka_broker_1";
    public static int KAFKA_BROKER_PORT = 9092;

    public static String KAFKA_CONNECT_SERVICE = "kafka_connect_1";
    public static int KAFKA_CONNECT_PORT = 8083;

    public static String KAFKA_SCHEMA_REG_SERVICE = "kafka_schema_registry_1";
    public static int KAFKA_SCHEMA_REG_PORT = 8081;

    public static String MONGODB_SERVICE = "mongodb_1";
    public static int MONGODB_PORT = 27017;

    @ClassRule
    public static DockerComposeContainer CONTAINER_ENV =
            new DockerComposeContainer(new File(COMPOSE_FILE))
                    .withExposedService(KAFKA_BROKER_SERVICE,KAFKA_BROKER_PORT)
                    .withExposedService(KAFKA_CONNECT_SERVICE,KAFKA_CONNECT_PORT)
                    .withExposedService(KAFKA_SCHEMA_REG_SERVICE,KAFKA_SCHEMA_REG_PORT)
                    .withExposedService(MONGODB_SERVICE,MONGODB_PORT)
            ;

    @BeforeAll
    public static void setup() {
        CONTAINER_ENV.starting(Description.EMPTY);
    }

    @AfterAll
    public static void tearDown() {
        //CONTAINER_ENV.finished(Description.EMPTY);
    }

    @Test
    @DisplayName("show connection urls for containerized services")
    public void showConnectionURLs() {

        System.out.println(
                KAFKA_BROKER_SERVICE + " -> " +
                        CONTAINER_ENV.getServiceHost(KAFKA_BROKER_SERVICE,KAFKA_BROKER_PORT)
                        + ": " +
                        CONTAINER_ENV.getServicePort(KAFKA_BROKER_SERVICE,KAFKA_BROKER_PORT)
        );

        System.out.println(
                KAFKA_CONNECT_SERVICE + " -> " +
                        CONTAINER_ENV.getServiceHost(KAFKA_CONNECT_SERVICE,KAFKA_CONNECT_PORT)
                        + ": " +
                        CONTAINER_ENV.getServicePort(KAFKA_CONNECT_SERVICE,KAFKA_CONNECT_PORT)
        );

        System.out.println(
                KAFKA_SCHEMA_REG_SERVICE + " -> " +
                        CONTAINER_ENV.getServiceHost(KAFKA_SCHEMA_REG_SERVICE,KAFKA_SCHEMA_REG_PORT)
                        + ": " +
                        CONTAINER_ENV.getServicePort(KAFKA_SCHEMA_REG_SERVICE,KAFKA_SCHEMA_REG_PORT)
        );

        System.out.println(
                MONGODB_SERVICE + " -> " +
                        CONTAINER_ENV.getServiceHost(MONGODB_SERVICE,MONGODB_PORT)
                        + ": " +
                        CONTAINER_ENV.getServicePort(MONGODB_SERVICE,MONGODB_PORT)
        );

        assert(true);
    }

    @Test
    @DisplayName("test producing record(s) to kafka and check resulting document(s) in mongodb sink")
    public void firstBasicTestE2E() throws IOException {

        //TODO: read this from config file
        String config = "{\"name\": \"e2e-test-mongo-sink\",\"config\": {\"connector.class\": \"at.grahsl.kafka.connect.mongodb.MongoDbSinkConnector\",  \"topics\": \"e2e-test-topic\",  \"mongodb.connection.uri\": \"mongodb://mongodb:27017/kafkaconnect?w=1&journal=true\",  \"mongodb.document.id.strategy\": \"at.grahsl.kafka.connect.mongodb.processor.id.strategy.ProvidedInValueStrategy\",  \"mongodb.collection\": \"e2e-test-collection\"}}";

        registerMongoDBSinkConnector(config);

        //TODO: read demo data from test files
        produceKafkaAvroRecord();

        //TODO: read back from MongoDB and verify results
    }

    private static void produceKafkaAvroRecord() {
        Properties props = new Properties();
        props.put("bootstrap.servers", CONTAINER_ENV.getServiceHost(KAFKA_BROKER_SERVICE,KAFKA_BROKER_PORT)
                + ":" +
                CONTAINER_ENV.getServicePort(KAFKA_BROKER_SERVICE,KAFKA_BROKER_PORT));
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://"+CONTAINER_ENV.getServiceHost(KAFKA_SCHEMA_REG_SERVICE,KAFKA_SCHEMA_REG_PORT)
                + ":" +
                CONTAINER_ENV.getServicePort(KAFKA_SCHEMA_REG_SERVICE,KAFKA_SCHEMA_REG_PORT));

        KafkaProducer<String, TweetMsg> producer = new KafkaProducer<>(props);

        for(int i = 0; i < 100; i++) {
            TweetMsg tweet = TweetMsg.newBuilder()
                    .setId$1(123456789000L+i)
                    .setText("test tweet "+(i+1)+": end2end testing apache kafka <-> mongodb sink connector is fun!")
                    .setHashtags(Arrays.asList(new String[]{"t"+i,"kafka","mongodb","testing"}))
                    .build();
            ProducerRecord<String, TweetMsg> record = new ProducerRecord<>("e2e-test-topic", tweet);
            System.out.println("producer sending -> " + tweet.toString());
            producer.send(record);
        }

        producer.close();
    }

    private static void registerMongoDBSinkConnector(String configuration) throws IOException {

        RequestBody body = RequestBody.create(
                MediaType.parse("application/json"), configuration
        );

        Request request = new Request.Builder()
                .url("http://"+CONTAINER_ENV.getServiceHost(KAFKA_CONNECT_SERVICE,KAFKA_CONNECT_PORT)
                        +":"+ CONTAINER_ENV.getServicePort(KAFKA_CONNECT_SERVICE,KAFKA_CONNECT_PORT) +"/connectors")
                .post(body)
                .build();

        Response response = new OkHttpClient().newCall(request).execute();
        assert(response.code() == 201);

    }

}
