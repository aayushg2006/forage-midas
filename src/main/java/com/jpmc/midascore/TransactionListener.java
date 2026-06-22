package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener { 

    private final UserRepository userRepository;
    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate;
    private final String incentiveUrl;

    public TransactionListener(
            UserRepository userRepository,
            DatabaseConduit databaseConduit,
            RestTemplateBuilder restTemplateBuilder,
            @Value("${general.incentive-url:http://localhost:8080/incentive}") String incentiveUrl) {
        this.userRepository = userRepository;
        this.databaseConduit = databaseConduit;
        this.restTemplate = restTemplateBuilder.build();
        this.incentiveUrl = incentiveUrl;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    @Transactional
    public void listen(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            return;
        }

        float amount = transaction.getAmount();
        if (amount <= 0 || sender.getBalance() < amount) {
            return;
        }

        float incentive = queryIncentive(transaction);

        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount + incentive);

        databaseConduit.save(sender);
        databaseConduit.save(recipient);
    }

    private float queryIncentive(Transaction transaction) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Transaction> request = new HttpEntity<>(transaction, headers);
        Balance response = restTemplate.postForObject(incentiveUrl, request, Balance.class);
        return response == null ? 0.0f : response.getAmount();
    }
}
