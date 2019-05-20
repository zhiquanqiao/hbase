package com.kteck.tools;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Worker implements Runnable {
    private String tableName;
    private List<ColumnMeta> tableColumn;
    private CountDownLatch counter;
    private long start;
    private long end;

    public Worker(long start, long end, String tableName, List<ColumnMeta> tableColumn, CountDownLatch counter) {
        this.start = start;
        this.end = end;
        this.tableName = tableName;
        this.tableColumn = tableColumn;
        this.counter = counter;
    }

    @Override
    public void run() {

        while (true) {
            boolean flag = false;
            List<List<ColumnValue>> tableValue = null;
            try {
                tableValue = OracleDao.getTableValue(tableName, start, end, tableColumn);
                flag = PhoenixDao.insert(tableValue, tableName);
            } catch (Exception e) {
                e.printStackTrace();
            }


            if (flag) break;
        }

        System.out.println("Thread id is :" + Thread.currentThread().getId() + "; Task id is: " + counter.getCount());
        counter.countDown();
    }
}
