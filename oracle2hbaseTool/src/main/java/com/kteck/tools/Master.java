package com.kteck.tools;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Master {


    private static int coreThreadSize = YamlReader.getInstance().getIntValueByKey("thread", "core");
    private static int maxThreadSize = YamlReader.getInstance().getIntValueByKey("thread", "max");
    private static String tableName = YamlReader.getInstance().getStringValueByKey("import", "tablename");
    private static int batchSize = YamlReader.getInstance().getIntValueByKey("import", "batchsize");
    private static ThreadPoolExecutor executor = new ThreadPoolExecutor(coreThreadSize, maxThreadSize, 10,
            TimeUnit.MINUTES, new LinkedBlockingQueue<>());


    private static CountDownLatch counter = null;


    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\dev tools\\hadoop-common-2.2.0-bin-master");
        long startTime = System.currentTimeMillis();
        int batchCount = getBatchCount();

        if (batchCount > 0) {
            counter = new CountDownLatch(batchCount);
            List<ColumnMeta> tableColumn = null;
            try {
                tableColumn = OracleDao.getTableColumns(tableName);
            } catch (Exception e) {
                e.printStackTrace();
            }
            for (int i = 0; i < batchCount; i++) {
                int start = i * batchSize;
                int end = (i + 1) * batchSize;
                executor.execute(new Worker(start, end, tableName, tableColumn, counter));
            }

            try {
                executor.shutdown();
                counter.await();
                System.out.println("Task finished, total execute time is:" + (System.currentTimeMillis() - startTime));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } else {
            return;
        }
    }


    private static int getBatchCount() {
        int sourceRowCount = 0;
        try {
            sourceRowCount = OracleDao.getSourceCount(tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourceRowCount % batchSize > 0 ?
                (int) Math.floor((double) sourceRowCount / (double) batchSize) : sourceRowCount / batchSize;
    }


}
