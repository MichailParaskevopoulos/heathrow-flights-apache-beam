package com.subscriber;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;

import com.google.cloud.pubsub.v1.Publisher;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.TopicName;
import com.google.pubsub.v1.PubsubMessage;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;

public class Subscriber implements HttpFunction {
    private static final Gson gson = new Gson();

    @Override
    public void service(HttpRequest request, HttpResponse response) throws IOException {
        BufferedWriter responseWriter = response.getWriter();

        String topicName =  //
        String topicId = //
        String projectId = //

        JsonObject body = gson.fromJson(request.getReader(), JsonObject.class);

        ByteString byteStr = ByteString.copyFrom(body.toString(), StandardCharsets.UTF_8);

        PubsubMessage pubsubApiMessage = PubsubMessage.newBuilder().setData(byteStr).build();
        Publisher publisher = Publisher.newBuilder(TopicName.of(projectId, topicId)).build();   

        String responseMessage;
        try {
            publisher.publish(pubsubApiMessage).get();
            responseMessage = "Message published.";
        } catch (InterruptedException | ExecutionException e) {
            responseMessage = "Error publishing Pub/Sub message; see logs for more info.";
        }

        responseWriter.write(responseMessage);

    }
}