package com.example.notification.channel;

import com.example.notification.domain.model.NotificationMessage;
import com.example.notification.domain.model.NotificationType;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmailChannel extends AbstractNotificationChannel {

    private final JavaMailSender mailSender;

    @Value("${notification.from-email}")
    private String fromEmail;

    @Value("${notification.from-name}")
    private String fromName;

    @Override
    public NotificationType supportedType() {
        return NotificationType.EMAIL;
    }

    @Override
    protected void validate(NotificationMessage message) {
        if (!StringUtils.hasText(message.getRecipientEmail())) {
            throw new IllegalArgumentException("Recipient email is required for EMAIL notification id=" + message.getNotificationId());
        }
    }

    @Override
    protected void doSend(NotificationMessage message) throws Exception {
        MimeMessage mimeMessage = mailSender.createMimeMessage();
        MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true, "UTF-8");

        helper.setFrom(fromEmail, fromName);
        helper.setTo(message.getRecipientEmail());
        helper.setSubject(message.getTitle());
        helper.setText(buildHtmlBody(message), true);

        mailSender.send(mimeMessage);
        log.debug("Email sent id={} to={}", message.getNotificationId(), message.getRecipientEmail());
    }

    private String buildHtmlBody(NotificationMessage message) {
        return """
                <html>
                  <body style="font-family: Arial, sans-serif; padding: 20px;">
                    <h2>%s</h2>
                    <p>%s</p>
                  </body>
                </html>
                """.formatted(message.getTitle(), message.getBody());
    }
}
