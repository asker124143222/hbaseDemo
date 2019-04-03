import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Author: xu.dm
 * @Date: 2019/4/2 18:42
 * @Description:
 * 1、cas原子操作测试
 */
public class HbaseDemo {

    static HBaseHelper helper;

    public static void main(String args[]) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.31.10");
        conf.set("hbase.rootdir", "file:///opt/hbase_data");
        conf.set("hbase.zookeeper.property.dataDir", "/opt/hbase_data/zookeeper");

        helper = HBaseHelper.getHBaseHelper(conf);

//        createDemoTable();

        CheckAndMutateExample();

    }

    //清除并插入测试数据
    private static void createDemoTable() throws IOException {
        String tableNameString = "demoTable";
        if (helper.existsTable(tableNameString))
            helper.dropTable(tableNameString);
        helper.createTable(tableNameString, 100, "cf1", "cf2");
        helper.put(tableNameString,
                new String[]{"row1"},
                new String[]{"cf1", "cf2"},
                new String[]{"qual1", "qual2", "qual3"},
                new long[]{1, 2, 3},
                new String[]{"val1", "val2", "val3"});
        System.out.println("Before check and mutate calls...");
        helper.dump("testtable", new String[]{"row1"}, null, null);
    }


    //测试操作原子性compare-and-set
    private static void CheckAndMutateExample() throws IOException {
        String tableNameString = "demoTable";
        Table table = helper.getConnection().getTable(TableName.valueOf(tableNameString));

        boolean res = false;
        Put put = null;

        put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("qual4"), 4, Bytes.toBytes("val1"));
//        如果row1 cf1 qual4 不存在值就插入put数据
        res = table.checkAndMutate(Bytes.toBytes("row1"), Bytes.toBytes("cf1"))
                .qualifier(Bytes.toBytes("qual4"))
                .ifNotExists()
                .thenPut(put);
        System.out.println("1 result is (expected true) :" + res);

        //如果row1 cf1 qual1 val1存在就插入put，因为这个value已经存在所以可以插入，结果返回true，时间戳变为4
//        res = table.checkAndMutate(Bytes.toBytes("row1"), Bytes.toBytes("cf1"))
//                .qualifier(Bytes.toBytes("qual1")).ifEquals(Bytes.toBytes("val1"))
//                .thenPut(put);
//        System.out.println("2 result is (expected true) :" + res);

//        put = new Put(Bytes.toBytes("row1"));
//        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("qual1"),4,Bytes.toBytes("val2"));
//        ////如果row1 cf1 qual1 不等于val2在就插入put
//        res = table.checkAndMutate(Bytes.toBytes("row1"), Bytes.toBytes("cf1"))
//                .qualifier(Bytes.toBytes("qual1"))
//                .ifMatches(CompareOperator.NOT_EQUAL,Bytes.toBytes("val2"))
//                .thenPut(put);
//        System.out.println("3 result is (expected true) :" + res);

        put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("qual5"),1,Bytes.toBytes("val1"));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("qual6"),1,Bytes.toBytes("val1"));

        Delete delete = new Delete(Bytes.toBytes("row1"));
        delete.addColumns(Bytes.toBytes("cf1"), Bytes.toBytes("qual4"));

        //RowMutations这个版本还没定型
        RowMutations mutations = new RowMutations(Bytes.toBytes("row1"));
        mutations.add(put);
        mutations.add(delete);

        //row1 cf1 qual4 val1存在,row1 cf1 qual5和row1 cf1 qual6无值则插入qual5和qual6的值，并删除qual4的值
        res = table.checkAndMutate(Bytes.toBytes("row1"),Bytes.toBytes("cf1")).qualifier(Bytes.toBytes("qual4"))
                .ifEquals(Bytes.toBytes("val1"))
                .qualifier(Bytes.toBytes("qual5")).ifNotExists()
                .qualifier(Bytes.toBytes("qual6")).ifNotExists()
                .thenMutate(mutations);
        System.out.println("1 result is (expected true) :" + res);


    }
}
