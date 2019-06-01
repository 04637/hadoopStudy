package zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.*;


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
        String path = "/mynode2";
        try{
            zooKeeper = new ZooKeeper("192.168.137.101:2181", SESSION_TIMEOUT, new ZKTest());
            zooKeeper.exists(path, true);
            zooKeeper.create(path, "mycontent1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            Thread.sleep(3000);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if(KeeperState.SyncConnected == watchedEvent.getState()){
            if(EventType.NodeCreated == watchedEvent.getType()){
                // 当节点创建成功时进行回调, 此处进行提示打印
                System.out.println("Node created success.\t"+watchedEvent.getPath());
                try {
                    zooKeeper.exists(watchedEvent.getPath(), true);
                } catch (KeeperException|InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
