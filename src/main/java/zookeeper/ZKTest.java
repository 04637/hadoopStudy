package zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.*;

import java.nio.charset.StandardCharsets;


/**
 * todo
 *
 * @author: 04637@163.com
 * @date: 2019/5/31
 */
public class ZKTest implements Watcher {
    private static final int SESSION_TIMEOUT = 30000;
    public static ZooKeeper zooKeeper;


    public static void main(String[] args) {
        String path = "/mynode3";
        try {
            zooKeeper = new ZooKeeper("192.168.137.101:2181", SESSION_TIMEOUT, new ZKTest());
            zooKeeper.exists(path, true);

            // 创建节点
            zooKeeper.create(path, "mycontent1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            Thread.sleep(3000);

            // 得到节点内容
            byte[] bytes1 = zooKeeper.getData(path, null, null);

            String result1 = new String(bytes1, StandardCharsets.UTF_8);
            System.out.println("result1: " + result1);

            // 设置节点内容
            zooKeeper.setData(path, "testSetData000".getBytes(StandardCharsets.UTF_8), -1);

            // 再次得到节点内容
            byte[] bytes2 = zooKeeper.getData(path, null, null);
            String result2 = new String(bytes2, StandardCharsets.UTF_8);

            System.out.println("result2: " + result2);

            Thread.sleep(3000);

            //删除节点
            zooKeeper.delete(path, -1);
            zooKeeper.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (KeeperState.SyncConnected == watchedEvent.getState()) {
            if (EventType.NodeCreated == watchedEvent.getType()) {
                // 当节点创建成功时进行回调, 此处进行提示打印
                System.out.println("Node created success.\t" + watchedEvent.getPath());
                try {
                    zooKeeper.exists(watchedEvent.getPath(), true);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }else if(EventType.NodeDeleted == watchedEvent.getType()){
                // 当节点删除
                try{
                    zooKeeper.exists(watchedEvent.getPath(), true);
                } catch (InterruptedException|KeeperException e) {
                    e.printStackTrace();
                }
                System.out.println("Node deleted success.");
            }else if(EventType.NodeDataChanged == watchedEvent.getType()){
                // 当数据变更
                try {
                    zooKeeper.exists(watchedEvent.getPath(), true);
                } catch (KeeperException|InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Node changed success");
            }
        }
    }
}
