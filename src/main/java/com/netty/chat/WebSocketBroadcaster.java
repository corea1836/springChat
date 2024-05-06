package com.netty.chat;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Component
@Slf4j
public class WebSocketBroadcaster extends TextWebSocketHandler {

    private final Map<String, WebSocketSession> sessionMap = new ConcurrentHashMap<>();
    private final BlockingQueue<String> messageQueue = new ArrayBlockingQueue<>(10000000);
    private final ScheduledExecutorService executorService =
            Executors.newScheduledThreadPool(1);

    @PostConstruct
    public void poll() {
        executorService.scheduleWithFixedDelay(() -> {
            List<String> messages = new ArrayList<>();
            try {
                messageQueue.drainTo(messages);
                messages.forEach(m -> this.broadcast(new TextMessage(m)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, 1, TimeUnit.MILLISECONDS);
    }

    public void enqueue(String message) {
        messageQueue.add(message);
    }
    public synchronized void broadcast(TextMessage message) {
        log.info(String.valueOf(sessionMap.size()));
        Long startTime = System.currentTimeMillis();
        sessionMap.values()
                .forEach(session -> {
                    try {
                        session.sendMessage(message);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
        Long endTime = System.currentTimeMillis();
        log.info("elapsed time : " + (endTime - startTime));
    }


    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws IOException {
        sessionMap.put(session.getId(), session);
        //session.sendMessage(new TextMessage("connected"));
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        broadcast(message);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws IOException {
        try (session) {
            sessionMap.remove(session.getId());
        }
    }
}
