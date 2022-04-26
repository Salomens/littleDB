package top.shi.mydb.backend.tbm;

import top.shi.mydb.backend.dm.DataManager;
import top.shi.mydb.backend.parser.statement.Begin;
import top.shi.mydb.backend.parser.statement.Create;
import top.shi.mydb.backend.parser.statement.Delete;
import top.shi.mydb.backend.parser.statement.Insert;
import top.shi.mydb.backend.parser.statement.Select;
import top.shi.mydb.backend.parser.statement.Update;
import top.shi.mydb.backend.utils.Parser;
import top.shi.mydb.backend.vm.VersionManager;
/*
        tableManager 实现了TBM.
        TBM用于管理表结构, 已经为上层模块提供更加高级和抽象的接口.
        TBM会依赖IM进行索引, 依赖SM进行表单数据查找.
        TBM本身的模型如下:
        [TBM] -> [Booter] -> [Table1] -> [Table2] -> [Table3] ...
        TBM将它管理的所有的表, 以链表的结构组织起来.
        并利用Booter, 存储了第一张表的UUID.
        TBM目前没有实现表的可见性管理, 也没有实现Drop语句.
        这样的目的是为了简洁代码.
        */
public interface TableManager {
    BeginRes begin(Begin begin);
    byte[] commit(long xid) throws Exception;
    byte[] abort(long xid);

    byte[] show(long xid);
    byte[] create(long xid, Create create) throws Exception;

    byte[] insert(long xid, Insert insert) throws Exception;
    byte[] read(long xid, Select select) throws Exception;
    byte[] update(long xid, Update update) throws Exception;
    byte[] delete(long xid, Delete delete) throws Exception;

    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
