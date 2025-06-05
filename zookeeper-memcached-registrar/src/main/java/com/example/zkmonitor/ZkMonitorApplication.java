package com.example.zkmonitor;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * ZkMonitorApplication: ZooKeeper 연결 → Watcher & HealthChecker 초기화 → 대기(종료되지 않음)
 *
 * 환경 변수:
 *   ZOOKEEPER_CONNECTION  = ZooKeeper 연결 문자열 (예: "zookeeper:2181" 또는 "localhost:2181")
 *   SPRING_NOTIFY_URL     = Spring 알림 API (예: "http://hashring-server:8080/zookeeper/nodeChange")
 *   MEMCACHED_BASE_NAME   = Memcached 호스트 접두사 (예: "memcached")
 *   MEMCACHED_COUNT       = 검사할 Memcached 노드 개수 (예: 4)
 *   MEMCACHED_PORT        = Memcached 내부 포트 (예: 11211)
 *   HEALTH_CHECK_INTERVAL = Health Check 주기(초) (예: 10)
 */
public class ZkMonitorApplication {
    private static final Logger log = LoggerFactory.getLogger(ZkMonitorApplication.class);

    public static void main(String[] args) throws Exception {
        // 1) 환경 변수 읽기 (기본값 설정 포함)
        String zkConn = System.getenv("ZOOKEEPER_CONNECTION");
        if (zkConn == null || zkConn.isBlank()) {
            zkConn = "localhost:2181";
            log.warn("ZOOKEEPER_CONNECTION 미설정. 기본값 사용: {}", zkConn);
        } else {
            log.info("ZOOKEEPER_CONNECTION={}", zkConn);
        }

        String springNotifyUrl = System.getenv("SPRING_NOTIFY_URL");
        if (springNotifyUrl == null || springNotifyUrl.isBlank()) {
            springNotifyUrl = "http://hashring-server:8080/zookeeper/nodeChange";
            log.warn("SPRING_NOTIFY_URL 미설정. 기본값 사용: {}", springNotifyUrl);
        } else {
            log.info("SPRING_NOTIFY_URL={}", springNotifyUrl);
        }

        String memBase = System.getenv("MEMCACHED_BASE_NAME");
        if (memBase == null || memBase.isBlank()) {
            memBase = "memcached";
            log.warn("MEMCACHED_BASE_NAME 미설정. 기본값 사용: {}", memBase);
        } else {
            log.info("MEMCACHED_BASE_NAME={}", memBase);
        }

        int memCount;
        try {
            memCount = Integer.parseInt(System.getenv("MEMCACHED_COUNT"));
        } catch (Exception e) {
            memCount = 4;
            log.warn("MEMCACHED_COUNT 미설정 또는 숫자 아님. 기본값 사용: {}", memCount);
        }
        log.info("MEMCACHED_COUNT={}", memCount);

        int memPort;
        try {
            memPort = Integer.parseInt(System.getenv("MEMCACHED_PORT"));
        } catch (Exception e) {
            memPort = 11211;
            log.warn("MEMCACHED_PORT 미설정 또는 숫자 아님. 기본값 사용: {}", memPort);
        }
        log.info("MEMCACHED_PORT={}", memPort);

        int intervalSec;
        try {
            intervalSec = Integer.parseInt(System.getenv("HEALTH_CHECK_INTERVAL"));
        } catch (Exception e) {
            intervalSec = 10;
            log.warn("HEALTH_CHECK_INTERVAL 미설정 또는 숫자 아님. 기본값 사용: {}", intervalSec);
        }
        log.info("HEALTH_CHECK_INTERVAL={}초", intervalSec);

        // 2) ZooKeeper 연결 (세션 타임아웃 5초, Watcher는 SyncConnected 감지만 처리)
        CountDownLatch connectedSignal = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper(zkConn, 5000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                log.info("ZooKeeper 연결 성공: {}");
                connectedSignal.countDown();
            } else if (event.getState() == Watcher.Event.KeeperState.Expired) {
                log.error("ZooKeeper 세션 만료");
                System.exit(1);
            }
        });
        connectedSignal.await();

        // 3) "/memcached/nodes" 경로 생성 보장
        String basePath = "/memcached/nodes";
        ZkUtilities.ensurePathExists(zk, basePath);

        // 4) ZkWatcher 초기화 및 Watcher 등록
        ZkWatcher watcher = new ZkWatcher(zk, basePath, springNotifyUrl);
        watcher.initializeAndWatch();

        // 5) HealthChecker 스케줄러 시작
        HealthChecker healthChecker = new HealthChecker(zk,
                basePath,
                memBase,
                memCount,
                memPort,
                intervalSec);
        healthChecker.start();

        // 6) 애플리케이션 종료되지 않도록 메인 쓰레드 대기
        synchronized (ZkMonitorApplication.class) {
            ZkMonitorApplication.class.wait();
        }

        // ※ 애플리케이션 종료 시 리소스 해제(만약 shutdown hook을 추가하고 싶다면)
        // healthChecker.stop();
        // zk.close();
    }
}