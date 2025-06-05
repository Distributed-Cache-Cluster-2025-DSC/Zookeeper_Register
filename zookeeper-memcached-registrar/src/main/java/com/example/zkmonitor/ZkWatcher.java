package com.example.zkmonitor;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ZooKeeper "/memcached/nodes" 경로 하위 znode 변화를 감시하는 Watcher.
 * - 자식 노드가 추가되면 Spring(Consistent Hash Ring)으로 UP 알림 전송
 * - 자식 노드가 삭제되면 Spring으로 DOWN 알림 전송
 */
public class ZkWatcher implements Watcher {
    private static final Logger log = LoggerFactory.getLogger(ZkWatcher.class);

    private final ZooKeeper zk;
    private final String watchPath;             // 예: "/memcached/nodes"
    private final String springNotifyUrl;       // 예: "http://hashring-server:8080/zookeeper/nodeChange"
    private final HttpClient httpClient;

    // 현재 ZooKeeper 상에 등록된 노드 목록을 메모리에 캐싱
    // (Set에 저장함으로써, 신규/삭제 노드 탐지)
    private final Set<String> currentNodes = ConcurrentHashMap.newKeySet();

    public ZkWatcher(ZooKeeper zk, String watchPath, String springNotifyUrl) {
        this.zk = zk;
        this.watchPath = watchPath;
        this.springNotifyUrl = springNotifyUrl;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
    }

    /**
     * 애플리케이션 시작 시 호출
     * 1) watchPath 하위 자식 노드 목록 조회 (watcher 등록 포함)
     * 2) 현재 자식 노드 목록을 Set에 저장
     * 3) (원한다면) 초기 목록을 기반으로 Spring UP 알림 전송 가능
     */
    public void initializeAndWatch() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(watchPath, this);
        log.info("[ZkWatcher] Initial children: {}", children);

        // 현재 노드 목록 초기화
        for (String child : children) {
            currentNodes.add(child);
            // 시작 시 이미 등록된 노드에 대해서도 Spring에 UP 알림을 보낼 수 있다. (옵션)
            sendAlert(child, true);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            // 1) 연결 상태(State) 이벤트는 무시
            if (event.getType() == Event.EventType.None) {
                return;
            }

            // 2) watchPath 경로 하위 자식 목록 변경 이벤트
            if (event.getType() == Event.EventType.NodeChildrenChanged
                    && Objects.equals(event.getPath(), watchPath)) {
                handleChildrenChanged();
            }
        } catch (Exception e) {
            log.error("[ZkWatcher] Error in process()", e);
        }
    }

    private void handleChildrenChanged() throws KeeperException, InterruptedException {
        // 1) Children 목록 재조회(Watcher 재등록)
        List<String> newChildren = zk.getChildren(watchPath, this);
        log.info("[ZkWatcher] Children changed: {}", newChildren);

        Set<String> newSet = new HashSet<>(newChildren);

        // 2) 신규 추가된 노드 탐지 (newSet 에만 있음)
        for (String added : newSet) {
            if (!currentNodes.contains(added)) {
                log.info("[ZkWatcher] Detected node UP: {}", added);
                currentNodes.add(added);
                sendAlert(added, true);
            }
        }

        // 3) 삭제된 노드 탐지 (currentNodes 에만 있었던 노드)
        for (String existed : List.copyOf(currentNodes)) {
            if (!newSet.contains(existed)) {
                log.info("[ZkWatcher] Detected node DOWN: {}", existed);
                currentNodes.remove(existed);
                sendAlert(existed, false);
            }
        }
    }

    /**
     * Spring 서버(Consistent Hash Ring) 로 HTTP POST 알림 전송
     * - JSON body: {"node":"nodeX","status":"UP" or "DOWN"}
     */
    private void sendAlert(String nodeName, boolean isUp) {
        try {
            String json = String.format(
                    "{\"node\":\"%s\",\"status\":\"%s\"}",
                    nodeName,
                    isUp ? "UP" : "DOWN"
            );

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(springNotifyUrl))
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json; charset=UTF-8")
                    .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                    .build();

            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenAccept(resp -> log.info("[ZkWatcher] Spring response: {} {}", resp.statusCode(), resp.body()))
                    .exceptionally(ex -> {
                        log.error("[ZkWatcher] Failed to notify Spring", ex);
                        return null;
                    });

            log.info("[ZkWatcher] Sent alert to Spring: node={}, up={}", nodeName, isUp);
        } catch (Exception e) {
            log.error("[ZkWatcher] Exception when sending alert", e);
        }
    }
}