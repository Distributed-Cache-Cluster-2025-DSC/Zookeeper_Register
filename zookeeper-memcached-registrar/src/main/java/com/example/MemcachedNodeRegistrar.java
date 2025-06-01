package com.example;

import org.apache.zookeeper.*;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;

public class MemcachedNodeRegistrar {

    private static final String ZK_HOST = "localhost:2181"; // Docker Compose 내 zookeeper 서비스 이름으로 변경될 수 있음
    private static final String BASE_PATH = "/memcached/nodes";
    private int memcachedPort = 11211; // 기본 포트, 필요시 변경 가능
    private String nodeId = "unknown"; // 기본 ID, 필요시 변경 가능
    private String memcachedHost = "localhost"; // 기본 호스트, 환경변수로 덮어쓰임

    private ZooKeeper zooKeeper;
    private String currentNodePath;
    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    public MemcachedNodeRegistrar(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.memcachedHost = host;
        this.memcachedPort = port;
    }

    public MemcachedNodeRegistrar() {
    }

    public static void main(String[] args) throws Exception {
        String nodeId = System.getenv("NODE_ID");
        String memcachedHost = System.getenv("MEMCACHED_HOST");
        String memcachedPortStr = System.getenv("MEMCACHED_PORT_ACTUAL");

        if (nodeId == null || nodeId.trim().isEmpty()) {
            System.err.println("NODE_ID 환경 변수가 설정되지 않았습니다. 기본값 'node-default'를 사용합니다.");
            nodeId = "node-default";
        }
        if (memcachedHost == null || memcachedHost.trim().isEmpty()) {
            System.err.println("MEMCACHED_HOST 환경 변수가 설정되지 않았습니다. 기본값 'localhost'를 사용합니다.");
            memcachedHost = "localhost"; 
        }
        int port = 11211;
        if (memcachedPortStr != null && !memcachedPortStr.trim().isEmpty()) {
            try {
                port = Integer.parseInt(memcachedPortStr);
            } catch (NumberFormatException e) {
                System.err.println("MEMCACHED_PORT_ACTUAL 환경 변수 값이 유효한 숫자가 아닙니다: " + memcachedPortStr + ". 기본 포트 " + port + "를 사용합니다.");
            }
        }

        MemcachedNodeRegistrar registrar = new MemcachedNodeRegistrar(nodeId, memcachedHost, port);
        registrar.start();

        Runtime.getRuntime().addShutdownHook(new Thread(registrar::close));
    }

    public void start() throws Exception {
        connectToZookeeper();
        connectedSignal.await();
        registerNode();
        keepAlive();
    }

    private void connectToZookeeper() throws IOException {
        String actualZkHost = System.getenv("ZOOKEEPER_CONNECTION");
        if (actualZkHost == null || actualZkHost.trim().isEmpty()) {
            System.out.println("ZOOKEEPER_CONNECTION 환경 변수가 없습니다. 기본값 " + ZK_HOST + "를 사용합니다.");
            actualZkHost = ZK_HOST;
        } else {
             System.out.println("ZOOKEEPER_CONNECTION 환경 변수 사용: " + actualZkHost);
        }

        final String finalZkHost = actualZkHost;

        this.zooKeeper = new ZooKeeper(finalZkHost, 5000, watchedEvent -> {
            if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
                System.out.println("ZooKeeper 연결 성공.");
                connectedSignal.countDown();
            } else if (watchedEvent.getState() == Watcher.Event.KeeperState.Expired) {
                System.out.println("ZooKeeper 세션 만료. 재연결 시도...");
                reconnect(finalZkHost);
            } else if (watchedEvent.getState() == Watcher.Event.KeeperState.Disconnected) {
                System.out.println("ZooKeeper 연결 끊김. 재연결 시도...");
            }
        });
    }

    private void ensureBasePath() throws KeeperException, InterruptedException {
        if (zooKeeper.exists(BASE_PATH, false) == null) {
            try {
                zooKeeper.create(BASE_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                System.out.println("Base path " + BASE_PATH + " 생성 완료.");
            } catch (KeeperException.NodeExistsException e) {
                System.out.println("Base path " + BASE_PATH + " 가 이미 존재합니다.");
            }
        }
    }

    private void registerNode() throws KeeperException, InterruptedException {
        ensureBasePath();
        String hostAddress = getHostAddress();
        String nodeData = hostAddress + " (ID: " + this.nodeId + ")";
        String specificNodePath = BASE_PATH + "/node-" + this.nodeId;

        if (zooKeeper.exists(specificNodePath, false) != null) {
            System.out.println("기존 노드 " + specificNodePath + "를 삭제하고 새로 등록합니다.");
            try {
                zooKeeper.delete(specificNodePath, -1);
            } catch (KeeperException.NoNodeException e) {
                System.out.println("삭제하려던 노드 " + specificNodePath + "가 이미 존재하지 않습니다.");
            }
        }

        currentNodePath = zooKeeper.create(
                specificNodePath,
                nodeData.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL
        );
        System.out.println("Registered node: " + currentNodePath + " with data: " + nodeData);
    }

    private void reconnect(String zkHost) {
        System.out.println("재연결 로직 시작...");
        try {
            if (zooKeeper != null) {
                zooKeeper.close();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("ZooKeeper 연결 해제 중 인터럽트 발생: " + e.getMessage());
        }
        try {
            connectToZookeeper();
            if (connectedSignal.getCount() == 0 || zooKeeper.getState() == ZooKeeper.States.CONNECTED) {
                 registerNode();
            } else {
                System.out.println("재연결 후에도 Zookeeper 연결 대기 중...");
                connectedSignal.await(); 
                registerNode();
            }
        } catch (Exception e) {
            System.err.println("ZooKeeper 재연결 및 재등록 실패: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private String getHostAddress() {
        // 환경 변수에서 MEMCACHED_HOST 와 MEMCACHED_PORT_ACTUAL (this.memcachedPort로 이미 설정됨) 을 사용
        return this.memcachedHost + ":" + this.memcachedPort;
    }

    private void keepAlive() throws InterruptedException {
        synchronized (this) {
            while (true) { 
                wait();
            }
        }
    }

    public void close() {
        System.out.println("애플리케이션 종료 중... ZooKeeper 연결을 해제합니다.");
        try {
            if (zooKeeper != null) {
                zooKeeper.close();
                System.out.println("ZooKeeper 연결이 성공적으로 해제되었습니다.");
            }
        } catch (InterruptedException e) {
            System.err.println("ZooKeeper 연결 해제 중 오류 발생: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
} 