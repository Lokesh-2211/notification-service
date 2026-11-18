package com.loki.notification.email;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

/**
 * Sends real emails using SMTP.
 * Where the email server is lives in application.yml (spring.mail.*).
 */
@Component
public class EmailSender {

    private final JavaMailSender mailSender;
    private final String from;

    public EmailSender(JavaMailSender mailSender,
                       @Value("${app.mail.from}") String from) {
        this.mailSender = mailSender;
        this.from = from;
    }

    public void send(String to, String subject, String body) {
        SimpleMailMessage email = new SimpleMailMessage();
        email.setFrom(from);
        email.setTo(to);
        email.setSubject(subject);
        email.setText(body);

        mailSender.send(email);
    }
}