package com.example.zkmonitor;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 주기적으로 실제 Memcached 호스트(예: memcached1, memcached2, ...) 에 TCP 연결 시도를 통해
 * “살아 있거나 죽었는지” 검사한 뒤,
 * ZooKeeper 상의 znode("/memcached/nodes/node-<ID>") 를
 *     - 살아 있으면 생성(createNode)
 *     - 죽었으면 삭제(deleteNode)
 * 하는 역할을 수행합니다.
 *
 * → 이때 znode 생성/삭제 시, ZkWatcher가 이를 감지하여 Spring으로 UP/DOWN 알림을 보내게 됩니다.
 */
public class HealthChecker {
    private static final Logger log = LoggerFactory.getLogger(HealthChecker.class);

    private final ZooKeeper zk;
    private final String basePath;        // "/memcached/nodes"
    private final String memcachedBase;   // 예: "memcached" (→ "memcached1", "memcached2", ...)
    private final int memcachedCount;     // 검사할 노드 개수 예: 4
    private final int memcachedPort;      // 기본 11211
    private final int intervalSeconds;    // Health Check 주기 (초 단위)

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public HealthChecker(ZooKeeper zk,
                         String basePath,
                         String memcachedBase,
                         int memcachedCount,
                         int memcachedPort,
                         int intervalSeconds) {
        this.zk = zk;
        this.basePath = basePath;
        this.memcachedBase = memcachedBase;
        this.memcachedCount = memcachedCount;
        this.memcachedPort = memcachedPort;
        this.intervalSeconds = intervalSeconds;
    }

    /**
     * Health Check 스케줄러 시작
     */
    public void start() {
        scheduler.scheduleAtFixedRate(this::checkAllNodes, 3, intervalSeconds, TimeUnit.SECONDS);
        log.info("[HealthChecker] Started with base='{}', count={}, port={}, interval={}s",
                memcachedBase, memcachedCount, memcachedPort, intervalSeconds);
    }

    /**
     * 모든 memcachedX 에 대해:
     * - TCP 연결이 가능하면 znode "/memcached/nodes/node-X" 생성
     * - 불가능하면 해당 znode 삭제
     */
    private void checkAllNodes() {
        for (int i = 1; i <= memcachedCount; i++) {
            String nodeId = "node" + i;                // znode 이름 suffix
            String host = memcachedBase + i;           // 예: "memcached" + "1" → "memcached1"
            boolean isAlive = isHostAlive(host, memcachedPort, 2000);

            String fullPath = basePath + "/" + nodeId; // "/memcached/nodes/node1" 등

            try {
                if (isAlive) {
                    // 살아 있으면 znode 존재 여부 확인 후, 없으면 생성
                    if (zk.exists(fullPath, false) == null) {
                        byte[] data = (host + ":" + memcachedPort).getBytes(StandardCharsets.UTF_8);
                        ZkUtilities.createNode(zk, fullPath, data);
                    }
                } else {
                    // 죽었으면 znode 가 있으면 삭제
                    if (zk.exists(fullPath, false) != null) {
                        ZkUtilities.deleteNode(zk, fullPath);
                    }
                }
            } catch (KeeperException | InterruptedException e) {
                log.error("[HealthChecker] Error syncing znode for {}:{}", host, memcachedPort, e);
            }
        }
    }

    /**
     * TCP 연결 시도(타임아웃 ms)하여 호스트 alive 여부 간단 검사
     */
    private boolean isHostAlive(String host, int port, int timeoutMillis) {
        try (Socket sock = new Socket()) {
            sock.connect(new InetSocketAddress(host, port), timeoutMillis);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 애플리케이션 종료 시 호출하여 스케줄러 종료
     */
    public void stop() {
        scheduler.shutdownNow();
    }
}