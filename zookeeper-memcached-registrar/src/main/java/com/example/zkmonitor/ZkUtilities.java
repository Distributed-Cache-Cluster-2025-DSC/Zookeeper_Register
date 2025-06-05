package com.example.zkmonitor;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * ZooKeeper 상에서 znode를 생성하거나 삭제하는 유틸리티 메서드
 */
public class ZkUtilities {
    /**
     * 지정된 znode 경로가 존재하지 않으면, 경로상의 모든 부모 노드를 차례로 생성한 뒤 마지막 노드를 PERSISTENT 모드로 생성
     * 예: ensurePathExists(zk, "/memcached/nodes");
     */
    public static void ensurePathExists(ZooKeeper zk, String fullPath)
            throws KeeperException, InterruptedException {
        // fullPath 예: "/memcached/nodes"
        String[] parts = fullPath.split("/");
        String pathSoFar = "";

        for (String part : parts) {
            if (part.isEmpty()) continue; // 맨 앞의 빈 문자열(루트) 무시
            pathSoFar += "/" + part;      // "/memcached" → "/memcached/nodes"

            if (zk.exists(pathSoFar, false) == null) {
                try {
                    zk.create(
                            pathSoFar,
                            new byte[0],
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT
                    );
                    System.out.println("[ZkUtilities] Created path: " + pathSoFar);
                } catch (KeeperException.NodeExistsException ignored) {
                    // 이미 다른 스레드에서 만든 경우 무시
                }
            }
        }
    }

    /**
     * nodeId 에 해당하는 znode를 생성 (혹은 이미 있으면 재생성)
     * 예: createNode(zk, "/memcached/nodes/node-node3", "memcached3:11211");
     */
    public static void createNode(ZooKeeper zk, String fullPath, byte[] data)
            throws KeeperException, InterruptedException {
        if (zk.exists(fullPath, false) != null) {
            // 이미 존재하면 삭제 후 새로 생성
            try {
                zk.delete(fullPath, -1);
                System.out.println("[ZkUtilities] Deleted existing znode: " + fullPath);
            } catch (KeeperException.NoNodeException ignored) {
                // 삭제하려던 노드가 없으면 무시
            }
        }
        zk.create(fullPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("[ZkUtilities] Created znode: " + fullPath + " -> " + new String(data));
    }

    /**
     * nodeId 에 해당하는 znode를 삭제 (존재할 때만)
     * 예: deleteNode(zk, "/memcached/nodes/node-node3");
     */
    public static void deleteNode(ZooKeeper zk, String fullPath) throws KeeperException, InterruptedException {
        if (zk.exists(fullPath, false) != null) {
            zk.delete(fullPath, -1);
            System.out.println("[ZkUtilities] Deleted znode: " + fullPath);
        }
    }
}