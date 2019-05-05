package com.gorbatenko.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.List;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String server = "127.0.0.1:2181";

        final Object lock = new Object();

        Watcher connectionWatcher = new Watcher() {
            public void process(WatchedEvent we) {
                if (we.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Connected to Zookeeper in " + Thread.currentThread().getName());
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                }
            }
        };

        int sessionTimeout = 2000;
        ZooKeeper zooKeeper = null;
        synchronized (lock) {
            zooKeeper = new ZooKeeper(server, sessionTimeout, connectionWatcher);
            lock.wait();
        }

        // Получение списка Child-znode's
        zooKeeper.getChildren("/RootZNode",null).forEach(System.out::println);

        // Создание нового узла
        String znodePath = "/zookeepernode2";
        List<ACL> acls = ZooDefs.Ids.OPEN_ACL_UNSAFE;
        if (zooKeeper.exists(znodePath, false) == null) {
            zooKeeper.create(znodePath, "data".getBytes(), acls, CreateMode.PERSISTENT);
        }

        // Получение данных из узла
        byte[] data = zooKeeper.getData(znodePath, null, null);
        System.out.println("Result: " + new String(data, "UTF-8"));

    }
}
