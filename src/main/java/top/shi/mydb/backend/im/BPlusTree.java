package top.shi.mydb.backend.im;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.shi.mydb.backend.common.SubArray;
import top.shi.mydb.backend.dm.DataManager;
import top.shi.mydb.backend.dm.dataItem.DataItem;
import top.shi.mydb.backend.im.Node.InsertAndSplitRes;
import top.shi.mydb.backend.im.Node.LeafSearchRangeRes;
import top.shi.mydb.backend.im.Node.SearchNextRes;
import top.shi.mydb.backend.tm.TransactionManagerImpl;
import top.shi.mydb.backend.utils.Parser;

/**
 *b+tree结构
 * 	每棵B+树都有一个bootUUID, 可通过它向DM读取该树的boot.
 * 	B+树boot里面存储了B+树根节点的地址.
 * 	PS: 因为B+树在算法执行过程中, 根节点可能会发生改变, 所以不能直接用根节点的地址当boot,
 * 	而需要一个固定的boot, 用来指向它的根节点.
 * 	PS: 目前B+树支持的最大键值为INF-1
 */
public class BPlusTree {
    DataManager dm;
    long bootUid;
    DataItem bootDataItem;
    Lock bootLock;
    // 创建一棵B+树, 并返回其bootUUID.
    public static long create(DataManager dm) throws Exception {
        byte[] rawRoot = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }
    // Load 通过BootUUID读取一课B+树, 并返回它.
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }
//5 存储根节点UID 因为b树再插入时会动态调整根节点
    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }
    // updaterootUUID 更新该树的根节点
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }
    // searchLeaf 根据key, 在nodeUUID代表节点的子树中搜索, 直到找到其对应的叶节点地址.
    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if(isLeaf) {
            return nodeUid;
        } else {
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }
    // serachNext 从nodeUUID对应节点开始, 不断的向右试探兄弟节点, 找到对应key的next uuid
    private long searchNext(long nodeUid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            SearchNextRes res = node.searchNext(key);
            node.release();
            if(res.uid != 0) return res.uid;
            nodeUid = res.siblingUid;
        }
    }

    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while(true) {// 不断的从leaf向sibling迭代, 将所有满足的uuid都加入
            Node leaf = Node.loadNode(this, leafUid);
            LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if(res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }
    // Insert 向B+树种插入(uuid, key)的键值对
    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if(res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    class InsertRes {
        long newNode, newKey;
    }
    // insert 将(uuid, key)插入到B+树中, 如果有分裂, 则将分裂产生的新节点也返回.
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if(isLeaf) {
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next, uid, key);
            if(ir.newNode != 0) {
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }
    // insertAndSplit 函数从node开始, 不断的向右试探兄弟节点, 直到找到一个节点, 能够插入进对应的值
    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if(iasr.siblingUid != 0) {// 继续向sibling尝试
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    public void close() {
        bootDataItem.release();
    }
}
