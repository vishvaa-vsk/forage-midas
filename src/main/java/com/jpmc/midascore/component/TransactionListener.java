package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.service.IncentiveService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class TransactionListener {

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final IncentiveService incentiveService;

    @Value("${general.kafka-topic}")
    private String topic;

    public TransactionListener(UserRepository userRepository, TransactionRepository transactionRepository, IncentiveService incentiveService) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.incentiveService = incentiveService;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {
        Optional<UserRecord> senderOpt = userRepository.findById(transaction.getSenderId());
        Optional<UserRecord> recipientOpt = userRepository.findById(transaction.getRecipientId());

        if (senderOpt.isPresent() && recipientOpt.isPresent()) {
            UserRecord sender = senderOpt.get();
            UserRecord recipient = recipientOpt.get();

            if (sender.getBalance() >= transaction.getAmount()) {
                sender.setBalance(sender.getBalance() - transaction.getAmount());
                recipient.setBalance(recipient.getBalance() + transaction.getAmount());

                Incentive incentive = incentiveService.getIncentive(transaction);
                recipient.setBalance(recipient.getBalance() + incentive.getAmount());

                TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount(), incentive.getAmount());
                transactionRepository.save(transactionRecord);

                userRepository.save(sender);
                userRepository.save(recipient);

                System.out.println("Transaction recorded: " + transaction);
            } else {
                System.out.println("Invalid transaction: " + transaction);
            }
        } else {
            System.out.println("Invalid transaction: " + transaction);
        }

        // Log the balance of the "wilbur" user
        logWilburBalance();
    }

    private void logWilburBalance() {
        Optional<UserRecord> wilburOpt = userRepository.findById(10L); // Assuming "wilbur" has ID 10
        if (wilburOpt.isPresent()) {
            UserRecord wilbur = wilburOpt.get();
            System.out.println("Wilbur's balance: " + Math.floor(wilbur.getBalance()));
        } else {
            System.out.println("Wilbur user not found.");
        }
    }
}
