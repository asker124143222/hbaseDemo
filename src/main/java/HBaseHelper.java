import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: xu.dm
 * @Date: 2019/3/24 18:23
 * @Description: Hbase辅助操作
 */
public class HBaseHelper implements Closeable {

    private Configuration configuration = null;
    private Connection connection = null;
    private Admin admin = null;

    private HBaseHelper(Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.connection = ConnectionFactory.createConnection(this.configuration);
        admin = this.connection.getAdmin();
    }

    public static HBaseHelper getHBaseHelper(Configuration configuration) throws IOException {
        return new HBaseHelper(configuration);
    }

    @Override
    public void close() throws IOException {
        admin.close();
        connection.close();
    }

    public Connection getConnection() {
        return connection;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void createNamespace(String namespace) {
        try {
            NamespaceDescriptor nd = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(nd);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    public void dropNamespace(String namespace, boolean force) {
        try {
            if (force) {
                TableName[] tableNames = admin.listTableNamesByNamespace(namespace);
                for (TableName name : tableNames) {
                    admin.disableTable(name);
                    admin.deleteTable(name);
                }
            }
        } catch (Exception e) {
            // ignore
        }
        try {
            admin.deleteNamespace(namespace);
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    public boolean existsTable(String table)
            throws IOException {
        return existsTable(TableName.valueOf(table));
    }

    public boolean existsTable(TableName table)
            throws IOException {
        return admin.tableExists(table);
    }

    public void createTable(String table, String... colfams)
            throws IOException {
        createTable(TableName.valueOf(table), 1, null, colfams);
    }

    public void createTable(TableName table, String... colfams)
            throws IOException {
        createTable(table, 1, null, colfams);
    }

    public void createTable(String table, int maxVersions, String... colfams)
            throws IOException {
        createTable(TableName.valueOf(table), maxVersions, null, colfams);
    }

    public void createTable(TableName table, int maxVersions, String... colfams)
            throws IOException {
        createTable(table, maxVersions, null, colfams);
    }

    public void createTable(String table, byte[][] splitKeys, String... colfams)
            throws IOException {
        createTable(TableName.valueOf(table), 1, splitKeys, colfams);
    }

    public void createTable(TableName table, int maxVersions, byte[][] splitKeys,
                            String... colfams)
            throws IOException {
        //表描述器构造器
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(table);

        //列族描述构造器
        ColumnFamilyDescriptorBuilder cfDescBuilder;

        //列描述器
        ColumnFamilyDescriptor cfDesc;


        for (String cf : colfams) {
            cfDescBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
            cfDescBuilder.setMaxVersions(maxVersions);
            cfDesc = cfDescBuilder.build();
            tableDescriptorBuilder.setColumnFamily(cfDesc);
        }
        //获得表描述器
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

        if (splitKeys != null) {
            admin.createTable(tableDescriptor, splitKeys);
        } else {
            admin.createTable(tableDescriptor);
        }
    }

    public void disableTable(String table) throws IOException {
        disableTable(TableName.valueOf(table));
    }

    public void disableTable(TableName table) throws IOException {
        admin.disableTable(table);
    }

    public void dropTable(String table) throws IOException {
        dropTable(TableName.valueOf(table));
    }

    public void dropTable(TableName table) throws IOException {
        if (existsTable(table)) {
            if (admin.isTableEnabled(table)) disableTable(table);
            admin.deleteTable(table);
        }
    }

    public void put(String table, String row, String fam, String qual,
                    String val) throws IOException {
        put(TableName.valueOf(table), row, fam, qual, val);
    }

    //插入或更新单行
    public void put(TableName table, String row, String fam, String qual,
                    String val) throws IOException {
        Table tbl = connection.getTable(table);
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), Bytes.toBytes(val));
        tbl.put(put);
        tbl.close();
    }

    public void put(String table, String row, String fam, String qual, long ts,
                    String val) throws IOException {
        put(TableName.valueOf(table), row, fam, qual, ts, val);
    }

    //带时间戳插入或更新单行
    public void put(TableName table, String row, String fam, String qual, long ts,
                    String val) throws IOException {
        Table tbl = connection.getTable(table);
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), ts,
                Bytes.toBytes(val));
        tbl.put(put);
        tbl.close();
    }

    //插入或者更新一个rowKey数据，一个Put里有一个rowKey，可能有多个列族和列名
    public void put(String tableNameString, Put put) throws IOException {
        TableName tableName = TableName.valueOf(tableNameString);
        Table table = connection.getTable(tableName);
        if (put != null && put.size() > 0) {
            table.put(put);
        }
        table.close();
    }

    public void put(String table, String[] rows, String[] fams, String[] quals,
                    long[] ts, String[] vals) throws IOException {
        put(TableName.valueOf(table), rows, fams, quals, ts, vals);
    }

    //用于测试数据
    public void put(TableName table, String[] rows, String[] fams, String[] quals,
                    long[] ts, String[] vals) throws IOException {
        Table tbl = connection.getTable(table);
        for (String row : rows) {
            Put put = new Put(Bytes.toBytes(row));
            for (String fam : fams) {
                int v = 0;
                for (String qual : quals) {
                    String val = vals[v < vals.length ? v : vals.length - 1];
                    long t = ts[v < ts.length ? v : ts.length - 1];
                    System.out.println("Adding: " + row + " " + fam + " " + qual +
                            " " + t + " " + val);
                    put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), t,
                            Bytes.toBytes(val));
                    v++;
                }
            }
            tbl.put(put);
        }
        tbl.close();
    }

    public void dump(String table, String[] rows, String[] fams, String[] quals)
            throws IOException {
        dump(TableName.valueOf(table), rows, fams, quals);
    }

    //测试dump数据
    public void dump(TableName table, String[] rows, String[] fams, String[] quals)
            throws IOException {
        Table tbl = connection.getTable(table);
        List<Get> gets = new ArrayList<Get>();
        for (String row : rows) {
            Get get = new Get(Bytes.toBytes(row));
            get.readAllVersions();
            if (fams != null) {
                for (String fam : fams) {
                    for (String qual : quals) {
                        get.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual));
                    }
                }
            }
            gets.add(get);
        }
        Result[] results = tbl.get(gets);
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell +
                        ", Value: " + Bytes.toString(cell.getValueArray(),
                        cell.getValueOffset(), cell.getValueLength()));
            }
        }
        tbl.close();
    }


    public void dump(String table) throws IOException {
        dump(TableName.valueOf(table));
    }

    public void dump(TableName table) throws IOException {
        try (
                Table t = connection.getTable(table);
                ResultScanner scanner = t.getScanner(new Scan());
        ) {
            for (Result result : scanner) {
                dumpResult(result);
            }
        }
    }

    //从Cell取Array要加上位移和长度，不然数据不正确
    public void dumpResult(Result result) {
        for (Cell cell : result.rawCells()) {
            System.out.println("Cell: " + cell +
                    ", Value: " + Bytes.toString(cell.getValueArray(),
                    cell.getValueOffset(), cell.getValueLength()));
        }
    }

    public void dumpCells(String key, List<Cell> list) {
        for (Cell cell : list) {
            String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            System.out.printf("[key:%s]\t[family:%s] [column:%s] [value:%s]\n",
                    key, columnFamily, columnName, value);
        }
    }


    //批量插入数据,list里每个map就是一条数据，并且按照rowKey columnFamily columnName columnValue放入map的key和value
    public void bulkInsert(String tableNameString, List<Map<String, Object>> list) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableNameString));
        List<Put> puts = new ArrayList<Put>();
        if (list != null && list.size() > 0) {
            for (Map<String, Object> map : list) {
                Put put = new Put(Bytes.toBytes(map.get("rowKey").toString()));
                put.addColumn(Bytes.toBytes(map.get("columnFamily").toString()),
                        Bytes.toBytes(map.get("columnName").toString()),
                        Bytes.toBytes(map.get("columnValue").toString()));
                puts.add(put);
            }
        }
        table.put(puts);
        table.close();
    }

    //批量插入
    public void bulkInsert2(String tableNameString, List<Put> puts) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableNameString));
        if (puts != null && puts.size() > 0) {
            table.put(puts);
        }
        table.close();
    }

    //根据rowKey删除所有行数据
    public void deleteByKey(String tableNameString, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableNameString));
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        table.delete(delete);
        table.close();
    }

    //根据rowKey和列族删除所有行数据
    public void deleteByKeyAndFamily(String tableNameString, String rowKey, String columnFamily) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableNameString));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addFamily(Bytes.toBytes(columnFamily));

        table.delete(delete);
        table.close();
    }

    //根据rowKey、列族删除多个列的数据
    public void deleteByKeyAndFC(String tableNameString, String rowKey,
                                 String columnFamily, List<String> columnNames) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableNameString));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        for (String columnName : columnNames) {
            delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        }
        table.delete(delete);
        table.close();
    }


    //根据rowkey，获取所有列族和列数据
    public List<Cell> getRowByKey(String tableNameString, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableNameString));

        Get get = new Get(Bytes.toBytes(rowKey));

        Result result = table.get(get);

//        Cell[] cells = result.rawCells();
        List<Cell> list = result.listCells();
        table.close();
        return list;
    }

    //根据rowKey，family,qualifier获取列值
    public List<Cell> getRowByKeyAndColumn(String tableNameString, String rowKey, String cf, String clName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableNameString));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(clName));

        Result result = table.get(get);
        List<Cell> list = result.listCells();
        table.close();
        return list;
    }

    //根据rowkey，获取所有列族和列数据
    public Map<String, List<Cell>> getRowByKeys(String tableNameString, String... rowKeys) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableNameString));

        List<Get> gets = new ArrayList<>();
        for (String rowKey : rowKeys) {
            Get get = new Get(Bytes.toBytes(rowKey));
            gets.add(get);
        }

        Result[] results = table.get(gets);

        Map<String, List<Cell>> map = new HashMap<>();
        for (Result res : results) {
            map.put(Bytes.toString(res.getRow()), res.listCells());
        }

        table.close();
        return map;
    }

    private Map<String, List<Cell>> formatToMap(String tableNameString,Scan scan) throws IOException{
        //确保table和scanner被释放
        try (Table table = connection.getTable(TableName.valueOf(tableNameString));
             ResultScanner scanner = table.getScanner(scan);
        ) {
            Map<String, List<Cell>> map = new HashMap<>();
            for (Result result : scanner) {
                map.put(Bytes.toString(result.getRow()), result.listCells());
            }
            return map;
        }
    }

    //根据rowKey过滤数据，rowKey可以使用正则表达式
    //返回rowKey和Cells的键值对
    public Map<String, List<Cell>> filterByRowKeyRegex(String tableNameString, String rowKey, CompareOperator operator) throws IOException {

        Scan scan = new Scan();
        //使用正则
        RowFilter filter = new RowFilter(operator, new RegexStringComparator(rowKey));

        //包含子串匹配,不区分大小写。
//        RowFilter filter = new RowFilter(operator,new SubstringComparator(rowKey));

        scan.setFilter(filter);

       return formatToMap(tableNameString,scan);
    }

    //根据列族，列名，列值（支持正则）查找数据
    //返回值：如果查询到值，会返回所有匹配的rowKey下的各列族、列名的所有数据（即使查询的时候这些列族和列名并不匹配）
    public Map<String, List<Cell>> filterByValueRegex(String tableNameString, String family, String colName,
                                                      String value, CompareOperator operator) throws IOException {
        Scan scan = new Scan();

        //正则匹配
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(family),
                Bytes.toBytes(colName), operator, new RegexStringComparator(value));

        //完全匹配
//        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(family),
//                Bytes.toBytes(colName),operator,Bytes.toBytes(value));

        //SingleColumnValueExcludeFilter排除列值

        //要过滤的列必须存在，如果不存在，那么这些列不存在的数据也会返回。如果不想让这些数据返回,设置setFilterIfMissing为true
        filter.setFilterIfMissing(true);
        scan.setFilter(filter);

        return formatToMap(tableNameString,scan);
    }

    //根据列名前缀过滤数据
    public Map<String, List<Cell>> filterByColumnPrefix(String tableNameString, String prefix) throws IOException {

        //列名前缀匹配
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes(prefix));

        //QualifierFilter 用于列名多样性匹配过滤
//        QualifierFilter filter = new QualifierFilter(CompareOperator.EQUAL,new SubstringComparator(prefix));

        //多个列名前缀匹配
//        MultipleColumnPrefixFilter multiFilter = new MultipleColumnPrefixFilter(new byte[][]{});

        Scan scan = new Scan();
        scan.setFilter(filter);

        return formatToMap(tableNameString,scan);
    }

    //根据列名范围以及列名前缀过滤数据
    public Map<String, List<Cell>> filterByPrefixAndRange(String tableNameString, String colPrefix,
                                                          String minCol, String maxCol) throws IOException {

        //列名前缀匹配
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes(colPrefix));

        //列名范围扫描，上下限范围包括
        ColumnRangeFilter rangeFilter = new ColumnRangeFilter(Bytes.toBytes(minCol), true,
                Bytes.toBytes(maxCol), true);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(filter);
        filterList.addFilter(rangeFilter);

        Scan scan = new Scan();
        scan.setFilter(filterList);

        return formatToMap(tableNameString,scan);
    }


}
