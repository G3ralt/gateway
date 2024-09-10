package com.example.service;

import com.example.model.ExternalServiceRequest;
import com.example.model.ExternalServiceResponse;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.List;

@Service
public class ExternalServiceCommunicator {

    private final WebClient.Builder webClientBuilder;
    private final RabbitTemplate rabbitTemplate;
    private final QueueService queueService;

    // List of external service URLs in application.properties
    // Or Get them dynamically from let's say a ExternalServicesManager if we are going to stop/start instances often
    @Value("${external.services.urls}")
    private List<String> externalServices;

    @Autowired
    public ExternalServiceCommunicator(WebClient.Builder webClientBuilder, RabbitTemplate rabbitTemplate, QueueService queueService) {
        this.webClientBuilder = webClientBuilder;
        this.rabbitTemplate = rabbitTemplate;
        this.queueService = queueService;
    }

    // Asynchronously send request to least busy external service
    public void sendRequestToExternalService(String requestId, Long sessionId) {
        String serviceUrl = queueService.getLeastBusyService(externalServices);
        if (serviceUrl == null) {
            throw new RuntimeException("No external services available.");
        }

        // Increment the queue size in Redis
        queueService.incrementQueueSize(serviceUrl);
        // Or send a message

        WebClient webClient = webClientBuilder.build();

        webClient.post()
                .uri(serviceUrl + "/handleRequest")
                .bodyValue(new ExternalServiceRequest(requestId, sessionId))
                .retrieve()
                .bodyToMono(ExternalServiceResponse.class)
                .subscribe(response -> {
                    // Handle the response and notify other instances
                    notifySiblings(response);

                    // Decrement the queue size in Redis
                    queueService.decrementQueueSize(serviceUrl);
                }, error -> {
                    // Log or handle the error
                    System.err.println("Error communicating with external service: " + error.getMessage());

                    // Notify other instances
                    notifySiblings(null);

                    // Decrement the queue size in Redis, even if there's an error
                    queueService.decrementQueueSize(serviceUrl);
                });
    }

    // Notify siblings through RabbitMQ after receiving a response from the external service
    // This could be done if we don't want to access redis for each request
    // Instead we will keep a local count for each queue and updated them when messages arrive
    private void notifySiblings(ExternalServiceResponse response) {
        rabbitTemplate.convertAndSend(response);
    }
}