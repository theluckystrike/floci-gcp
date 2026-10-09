package io.floci.gcp.services.pubsub;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

@QuarkusTest
class PubSubRestIntegrationTest {

    @Test
    void terraformStylePubSubRestCrudAndPublishPullWork() {
        String project = "pubsub-rest-it";
        String topic = "orders-events";
        String subscription = "orders-events-to-bigquery";
        String topicName = "projects/" + project + "/topics/" + topic;
        String subscriptionName = "projects/" + project + "/subscriptions/" + subscription;

        given()
                .contentType("application/json")
                .body("""
                        {
                          "labels": {"app": "orders", "env": "local"},
                          "messageRetentionDuration": "604800s"
                        }
                        """)
                .when().put("/v1/projects/" + project + "/topics/" + topic)
                .then()
                .statusCode(200)
                .body("name", equalTo(topicName))
                .body("labels.app", equalTo("orders"))
                .body("messageRetentionDuration", equalTo("604800s"));

        given()
                .when().get("/v1/projects/" + project + "/topics")
                .then()
                .statusCode(200)
                .body("topics.name", hasItem(topicName));

        given()
                .contentType("application/json")
                .body("""
                        {
                          "topic": {
                            "labels": {"app": "orders", "env": "patched"},
                            "messageRetentionDuration": "1200s"
                          },
                          "updateMask": "labels,messageRetentionDuration"
                        }
                        """)
                .when().patch("/v1/projects/" + project + "/topics/" + topic)
                .then()
                .statusCode(200)
                .body("labels.env", equalTo("patched"))
                .body("messageRetentionDuration", equalTo("1200s"));

        given()
                .contentType("application/json")
                .body("""
                        {
                          "topic": "%s",
                          "ackDeadlineSeconds": 10,
                          "messageRetentionDuration": "604800s",
                          "labels": {"app": "orders", "sink": "bigquery"},
                          "bigqueryConfig": {
                            "table": "%s.orders_analytics.order_events",
                            "useTableSchema": true,
                            "writeMetadata": false,
                            "dropUnknownFields": true
                          }
                        }
                        """.formatted(topicName, project))
                .when().put("/v1/projects/" + project + "/subscriptions/" + subscription)
                .then()
                .statusCode(200)
                .body("name", equalTo(subscriptionName))
                .body("topic", equalTo(topicName))
                .body("ackDeadlineSeconds", equalTo(10))
                .body("labels.sink", equalTo("bigquery"))
                .body("bigqueryConfig.table", equalTo(project + ".orders_analytics.order_events"))
                .body("bigqueryConfig.state", equalTo("ACTIVE"));

        given()
                .contentType("application/json")
                .queryParam("updateMask", "labels,ackDeadlineSeconds")
                .body("""
                        {
                          "ackDeadlineSeconds": 20,
                          "labels": {"app": "orders", "sink": "worker"}
                        }
                        """)
                .when().patch("/v1/projects/" + project + "/subscriptions/" + subscription)
                .then()
                .statusCode(200)
                .body("ackDeadlineSeconds", equalTo(20))
                .body("labels.sink", equalTo("worker"))
                .body("bigqueryConfig.table", equalTo(project + ".orders_analytics.order_events"));

        given()
                .contentType("application/json")
                .body("""
                        {
                          "subscription": {
                            "ackDeadlineSeconds": 30,
                            "labels": {"app": "orders", "sink": "canonical"},
                            "retainAckedMessages": true,
                            "bigqueryConfig": {
                              "table": "%s.orders_analytics.order_events_v2",
                              "useTableSchema": true
                            }
                          },
                          "updateMask": "ackDeadlineSeconds,labels,retainAckedMessages,bigqueryConfig"
                        }
                        """.formatted(project))
                .when().patch("/v1/projects/" + project + "/subscriptions/" + subscription)
                .then()
                .statusCode(200)
                .body("ackDeadlineSeconds", equalTo(30))
                .body("labels.sink", equalTo("canonical"))
                .body("retainAckedMessages", equalTo(true))
                .body("bigqueryConfig.table", equalTo(project + ".orders_analytics.order_events_v2"))
                .body("bigqueryConfig.state", equalTo("ACTIVE"));

        String payload = Base64.getEncoder().encodeToString("hello from rest".getBytes());
        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("""
                        {
                          "messages": [
                            {"data": "%s", "attributes": {"event_type": "created"}}
                          ]
                        }
                        """.formatted(payload))
                .when().post("/v1/projects/" + project + "/topics/" + topic + ":publish")
                .then()
                .statusCode(200)
                .body("messageIds.size()", equalTo(1));

        String ackId = given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{\"maxMessages\": 1}")
                .when().post("/v1/projects/" + project + "/subscriptions/" + subscription + ":pull")
                .then()
                .statusCode(200)
                .body("receivedMessages.size()", equalTo(1))
                .body("receivedMessages[0].message.data", equalTo(payload))
                .body("receivedMessages[0].message.attributes.event_type", equalTo("created"))
                .extract().path("receivedMessages[0].ackId");

        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{\"ackIds\": [\"" + ackId + "\"]}")
                .when().post("/v1/projects/" + project + "/subscriptions/" + subscription + ":acknowledge")
                .then()
                .statusCode(200)
                .body("$", anEmptyMap());

        given()
                .when().get("/v1/projects/" + project + "/subscriptions")
                .then()
                .statusCode(200)
                .body("subscriptions.name", hasItem(subscriptionName));

        given()
                .when().delete("/v1/projects/" + project + "/subscriptions/" + subscription)
                .then()
                .statusCode(200)
                .body("$", anEmptyMap());

        given()
                .when().delete("/v1/projects/" + project + "/topics/" + topic)
                .then()
                .statusCode(200)
                .body("$", anEmptyMap());
    }

    @Test
    void filteredSubscriptionOnlyReceivesMatchingMessages() {
        String project = "pubsub-rest-filter-it";
        String topic = "events";
        String base = "/v1/projects/" + project;

        given().when().put(base + "/topics/" + topic).then().statusCode(200);

        given()
                .contentType("application/json")
                .body("""
                        {
                          "topic": "projects/%s/topics/%s",
                          "filter": "attributes.event_type = \\"ocr-invoice\\""
                        }
                        """.formatted(project, topic))
                .when().put(base + "/subscriptions/filtered")
                .then()
                .statusCode(200)
                .body("filter", equalTo("attributes.event_type = \"ocr-invoice\""));

        given()
                .contentType("application/json")
                .body("{\"topic\": \"projects/%s/topics/%s\"}".formatted(project, topic))
                .when().put(base + "/subscriptions/unfiltered")
                .then()
                .statusCode(200);

        String payload = Base64.getEncoder().encodeToString("body".getBytes());
        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("""
                        {
                          "messages": [
                            {"data": "%s", "attributes": {"event_type": "portal.upload"}},
                            {"data": "%s", "attributes": {"event_type": "ocr-invoice"}}
                          ]
                        }
                        """.formatted(payload, payload))
                .when().post(base + "/topics/" + topic + ":publish")
                .then()
                .statusCode(200)
                .body("messageIds.size()", equalTo(2));

        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{\"maxMessages\": 10}")
                .when().post(base + "/subscriptions/filtered:pull")
                .then()
                .statusCode(200)
                .body("receivedMessages.size()", equalTo(1))
                .body("receivedMessages[0].message.attributes.event_type", equalTo("ocr-invoice"));

        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{\"maxMessages\": 10}")
                .when().post(base + "/subscriptions/unfiltered:pull")
                .then()
                .statusCode(200)
                .body("receivedMessages.size()", equalTo(2));
    }

    @Test
    void patchingTheFilterIsRejectedWithInvalidArgument() {
        String project = "pubsub-rest-filter-immutable-it";
        String base = "/v1/projects/" + project;

        given().when().put(base + "/topics/events").then().statusCode(200);

        given()
                .contentType("application/json")
                .body("""
                        {
                          "topic": "projects/%s/topics/events",
                          "filter": "attributes.event_type = \\"a\\""
                        }
                        """.formatted(project))
                .when().put(base + "/subscriptions/immutable")
                .then()
                .statusCode(200);

        given()
                .contentType("application/json")
                .queryParam("updateMask", "filter")
                .body("{\"filter\": \"attributes.event_type = \\\"b\\\"\"}")
                .when().patch(base + "/subscriptions/immutable")
                .then()
                .statusCode(400)
                .body("error.status", equalTo("INVALID_ARGUMENT"));

        given()
                .contentType("application/json")
                .queryParam("updateMask", "filter")
                .body("{\"filter\": \"attributes.event_type = \\\"a\\\"\"}")
                .when().patch(base + "/subscriptions/immutable")
                .then()
                .statusCode(400)
                .body("error.status", equalTo("INVALID_ARGUMENT"));

        given()
                .when().get(base + "/subscriptions/immutable")
                .then()
                .statusCode(200)
                .body("filter", equalTo("attributes.event_type = \"a\""));

        given()
                .contentType("application/json")
                .queryParam("updateMask", "labels")
                .body("{\"labels\": {\"env\": \"local\"}}")
                .when().patch(base + "/subscriptions/immutable")
                .then()
                .statusCode(200)
                .body("labels.env", equalTo("local"))
                .body("filter", equalTo("attributes.event_type = \"a\""));

        given()
                .contentType("application/json")
                .queryParam("updateMask", "labels")
                .body("""
                        {
                          "labels": {"env": "echoed"},
                          "filter": "attributes.event_type = \\"ignored\\""
                        }
                        """)
                .when().patch(base + "/subscriptions/immutable")
                .then()
                .statusCode(200)
                .body("labels.env", equalTo("echoed"))
                .body("filter", equalTo("attributes.event_type = \"a\""));
    }

    @Test
    void publishAcceptsUrlSafeUnpaddedBase64Data() {
        String project = "pubsub-rest-url-safe-it";
        String base = "/v1/projects/" + project;

        given().when().put(base + "/topics/events").then().statusCode(200);
        given()
                .contentType("application/json")
                .body("{\"topic\": \"projects/%s/topics/events\"}".formatted(project))
                .when().put(base + "/subscriptions/all")
                .then()
                .statusCode(200);

        // 0xfb 0xff encodes as "+/8=" in standard base64 and "-_8" in URL-safe unpadded form.
        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{\"messages\": [{\"data\": \"-_8\"}]}")
                .when().post(base + "/topics/events:publish")
                .then()
                .statusCode(200)
                .body("messageIds.size()", equalTo(1));

        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{\"maxMessages\": 10}")
                .when().post(base + "/subscriptions/all:pull")
                .then()
                .statusCode(200)
                .body("receivedMessages.size()", equalTo(1))
                .body("receivedMessages[0].message.data", equalTo("+/8="));
    }

    @Test
    void publishRejectsUndecodableDataWithInvalidArgumentAndPublishesNothing() {
        String project = "pubsub-rest-bad-base64-it";
        String base = "/v1/projects/" + project;

        given().when().put(base + "/topics/events").then().statusCode(200);
        given()
                .contentType("application/json")
                .body("{\"topic\": \"projects/%s/topics/events\"}".formatted(project))
                .when().put(base + "/subscriptions/all")
                .then()
                .statusCode(200);

        // The valid first message must not be published when the second cannot be decoded.
        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{\"messages\": [{\"data\": \"+/8=\"}, {\"data\": \"not*base64\"}]}")
                .when().post(base + "/topics/events:publish")
                .then()
                .statusCode(400)
                .body("error.code", equalTo(400))
                .body("error.status", equalTo("INVALID_ARGUMENT"));

        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{\"maxMessages\": 10}")
                .when().post(base + "/subscriptions/all:pull")
                .then()
                .statusCode(200)
                .body("receivedMessages", empty());
    }

    @Test
    void unparseableFilterIsRejectedWithInvalidArgument() {
        String project = "pubsub-rest-filter-invalid-it";
        String base = "/v1/projects/" + project;

        given().when().put(base + "/topics/events").then().statusCode(200);

        given()
                .contentType("application/json")
                .body("""
                        {
                          "topic": "projects/%s/topics/events",
                          "filter": "this is not a filter ((("
                        }
                        """.formatted(project))
                .when().put(base + "/subscriptions/bad")
                .then()
                .statusCode(400)
                .body("error.code", equalTo(400))
                .body("error.status", equalTo("INVALID_ARGUMENT"));

        given()
                .when().get(base + "/subscriptions/bad")
                .then()
                .statusCode(404);
    }

    @Test
    void filterOverTheByteLimitIsRejectedWithInvalidArgument() {
        String project = "pubsub-rest-filter-length-it";
        String base = "/v1/projects/" + project;

        given().when().put(base + "/topics/events").then().statusCode(200);

        given()
                .contentType("application/json")
                .body("""
                        {
                          "topic": "projects/%s/topics/events",
                          "filter": "attributes.name = \\"%s\\""
                        }
                        """.formatted(project, "x".repeat(300)))
                .when().put(base + "/subscriptions/too-long")
                .then()
                .statusCode(400)
                .body("error.status", equalTo("INVALID_ARGUMENT"));
    }

    // Regression: before IAM routing existed, :getIamPolicy fell into the plain
    // topic GET route and reported an existing topic as "Topic not found".
    @Test
    void getIamPolicyOnExistingTopicReturnsEmptyPolicyNotTopicNotFound() {
        String project = "pubsub-rest-iam-it";
        String base = "/v1/projects/" + project;

        given().when().put(base + "/topics/iam-target").then().statusCode(200);

        given()
                .urlEncodingEnabled(false)
                .when().get(base + "/topics/iam-target:getIamPolicy")
                .then()
                .statusCode(200)
                .body("etag", equalTo("ACAB"))
                .body("bindings", empty());
    }

    @Test
    void topicIamPolicySetReadBackAndClearedOnDelete() {
        String project = "pubsub-rest-iam-crud-it";
        String base = "/v1/projects/" + project;

        given().when().put(base + "/topics/orders").then().statusCode(200);

        String etag = given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("""
                        {
                          "policy": {
                            "bindings": [
                              {
                                "role": "roles/pubsub.publisher",
                                "members": ["serviceAccount:worker@%s.iam.gserviceaccount.com"]
                              }
                            ]
                          }
                        }
                        """.formatted(project))
                .when().post(base + "/topics/orders:setIamPolicy")
                .then()
                .statusCode(200)
                .body("bindings[0].role", equalTo("roles/pubsub.publisher"))
                .extract().path("etag");

        given()
                .urlEncodingEnabled(false)
                .when().get(base + "/topics/orders:getIamPolicy")
                .then()
                .statusCode(200)
                .body("etag", equalTo(etag))
                .body("bindings[0].members[0]",
                        equalTo("serviceAccount:worker@" + project + ".iam.gserviceaccount.com"));

        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{\"policy\": {\"etag\": \"bm90LXRoZS1ldGFn\"}}")
                .when().post(base + "/topics/orders:setIamPolicy")
                .then()
                .statusCode(409)
                .body("error.status", equalTo("ABORTED"));

        given().when().delete(base + "/topics/orders").then().statusCode(200);
        given().when().put(base + "/topics/orders").then().statusCode(200);

        given()
                .urlEncodingEnabled(false)
                .when().get(base + "/topics/orders:getIamPolicy")
                .then()
                .statusCode(200)
                .body("etag", equalTo("ACAB"))
                .body("bindings", empty());
    }

    @Test
    void iamPolicyOnMissingTopicIsNotFound() {
        String project = "pubsub-rest-iam-missing-it";
        String base = "/v1/projects/" + project;

        given()
                .urlEncodingEnabled(false)
                .when().get(base + "/topics/never-created:getIamPolicy")
                .then()
                .statusCode(404)
                .body("error.status", equalTo("NOT_FOUND"));

        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{\"policy\": {}}")
                .when().post(base + "/topics/never-created:setIamPolicy")
                .then()
                .statusCode(404)
                .body("error.status", equalTo("NOT_FOUND"));
    }

    @Test
    void subscriptionIamPolicyIndependentOfTopic() {
        String project = "pubsub-rest-iam-sub-it";
        String base = "/v1/projects/" + project;

        given().when().put(base + "/topics/shared").then().statusCode(200);
        given()
                .contentType("application/json")
                .body("{\"topic\": \"projects/" + project + "/topics/shared\"}")
                .when().put(base + "/subscriptions/shared")
                .then()
                .statusCode(200);

        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("""
                        {
                          "policy": {
                            "bindings": [
                              {"role": "roles/pubsub.subscriber", "members": ["user:sub@example.com"]}
                            ]
                          }
                        }
                        """)
                .when().post(base + "/subscriptions/shared:setIamPolicy")
                .then()
                .statusCode(200);

        given()
                .urlEncodingEnabled(false)
                .when().get(base + "/topics/shared:getIamPolicy")
                .then()
                .statusCode(200)
                .body("bindings", empty());

        given()
                .urlEncodingEnabled(false)
                .when().get(base + "/subscriptions/shared:getIamPolicy")
                .then()
                .statusCode(200)
                .body("bindings[0].role", equalTo("roles/pubsub.subscriber"));
    }

    @Test
    void testIamPermissionsEchoesForExistingTopicAndFailsOpenEmptyForMissing() {
        String project = "pubsub-rest-iam-perm-it";
        String base = "/v1/projects/" + project;

        given().when().put(base + "/topics/perm-target").then().statusCode(200);

        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{\"permissions\": [\"pubsub.topics.publish\", \"pubsub.topics.get\"]}")
                .when().post(base + "/topics/perm-target:testIamPermissions")
                .then()
                .statusCode(200)
                .body("permissions[0]", equalTo("pubsub.topics.publish"))
                .body("permissions[1]", equalTo("pubsub.topics.get"));

        // Missing resource fails open: empty permission set, not NOT_FOUND.
        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{\"permissions\": [\"pubsub.topics.publish\"]}")
                .when().post(base + "/topics/never-created:testIamPermissions")
                .then()
                .statusCode(200)
                .body("permissions", empty());
    }

    @Test
    void seekToTimeDropsOlderMessagesAndBadTimeIsRejected() {
        String project = "pubsub-rest-seek-it";
        String topic = "seek-events";
        String subscription = "seek-events-sub";
        String base = "/v1/projects/" + project;

        given().when().put(base + "/topics/" + topic).then().statusCode(200);

        given()
                .contentType("application/json")
                .body("{\"topic\": \"projects/%s/topics/%s\"}".formatted(project, topic))
                .when().put(base + "/subscriptions/" + subscription)
                .then()
                .statusCode(200);

        String oldPayload = Base64.getEncoder().encodeToString("old".getBytes());
        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{\"messages\": [{\"data\": \"%s\"}]}".formatted(oldPayload))
                .when().post(base + "/topics/" + topic + ":publish")
                .then()
                .statusCode(200);

        // Seek to a far-future time: every message published before it is treated as acknowledged
        // and dropped, so only messages published after the seek remain pullable.
        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{\"time\": \"2099-01-01T00:00:00Z\"}")
                .when().post(base + "/subscriptions/" + subscription + ":seek")
                .then()
                .statusCode(200)
                .body("$", anEmptyMap());

        String newPayload = Base64.getEncoder().encodeToString("new".getBytes());
        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{\"messages\": [{\"data\": \"%s\"}]}".formatted(newPayload))
                .when().post(base + "/topics/" + topic + ":publish")
                .then()
                .statusCode(200);

        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{\"maxMessages\": 10}")
                .when().post(base + "/subscriptions/" + subscription + ":pull")
                .then()
                .statusCode(200)
                .body("receivedMessages.size()", equalTo(1))
                .body("receivedMessages[0].message.data", equalTo(newPayload));

        // A malformed seek time is rejected with 400 INVALID_ARGUMENT, like the other routes
        // map their errors.
        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{\"time\": \"not-a-valid-time\"}")
                .when().post(base + "/subscriptions/" + subscription + ":seek")
                .then()
                .statusCode(400);
    }
}
