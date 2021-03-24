
package tz15_bigdata_root.hbase.insert;


import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.regionserver.HRegion;
import tz15_bigdata_root.hbase.config.HBaseTableUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @version : 1.0
 * @description 添加HBASE 插入数据类
 * @Date 09:33 2017/7/18
 * @auth :
 */
public class HBaseInsertHelper implements Serializable {

	private HBaseInsertHelper(){}

	/**
	 * 将单条 put存入List
	 * @param tableName
	 * @param put
	 * @throws Exception
	 */
	public static void put(String tableName, Put put) throws Exception {
		put(tableName, Lists.newArrayList(put));
	}

	/**
	 *  批量写
	 * @param tableName  表名
	 * @param puts       list<PUT>
	 * @throws Exception
	 */
	public static void put(String tableName, List<Put> puts) throws Exception {
		System.out.println("批量写========初始化ing……………………");
		if(!puts.isEmpty()){
			System.out.println("批量写"+puts);
			//获取hbase表连接
			Table table = HBaseTableUtil.getTable(tableName);
			System.out.println("table:gettable"+table);
			try {
				System.out.println(table.getName().getNameAsString());
				System.out.println("获取表名，并写入单条数据"+table);
				table.put(puts);
				System.out.println("成功写入数据");

			}catch (Exception e){
				System.out.println("异常跳出");
				e.printStackTrace();
			}finally {
				System.out.println("关闭表连接");
				HBaseTableUtil.close(table);
			}
		}
 	}


	/**
	 * 大批量写入  并发写入
	 * @param tableName     表名
	 * @param puts           List<Put> puts
	 * @param perThreadPutSize    每个线程分多少条数据
	 * @throws Exception
	 */
	public static void put(final String tableName,
						   List<Put> puts,
						   int perThreadPutSize) throws Exception {
		System.out.println(" 大批量写入  ====并发写入==============");
		// 获取list大小
		int size = puts.size();
		//如果每批数据大于  单个线程数据量  那么进行线程拆分
		if(size > perThreadPutSize){
			//计算线程数
			int threadNum = (int)Math.ceil(size / (double)perThreadPutSize);
			//创建一个固定线程池 10
			ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
			//定义一个线程屏障
			final CountDownLatch  cdl = new CountDownLatch(threadNum);
			final List<Exception>  es = Collections.synchronizedList(new ArrayList<Exception>());
			try {
				for(int i = 0; i < threadNum; i++){
					final List<Put> tmp;
					//第一次  i=0
					if(i == (threadNum - 1)){
						//数据切分
						tmp = puts.subList(perThreadPutSize*i, size);
					}else{
						tmp = puts.subList(perThreadPutSize*i, perThreadPutSize*(i + 1));
					}
					executorService.execute(new Runnable() {
						@Override
						public void run() {
							try {
								if(es.isEmpty()) {
									put(tableName, tmp);
								}
							} catch (Exception e) {
								es.add(e);
							}finally {
								cdl.countDown();
							}
						}
					});
				}
				cdl.await();
			}finally {
				executorService.shutdown();
			}
			if(es.size() > 0){
				HBaseInsertException insertException = new HBaseInsertException(String.format("put数据到表%s失败。"));
				insertException.addSuppresseds(es);
				throw insertException;
			}
		}else {
			put(tableName, puts);
		}
	}



	public static void checkAndPut(String tableName, byte[] row, byte[] family, byte[] qualifier,
								   byte[] value, Put put) throws Exception {
		checkAndPut(tableName, row, family, qualifier, null, value, put);
	}

	public static void checkAndPut(String tableName, byte[] row, byte[] family, byte[] qualifier,
								  CompareOp compareOp, byte[] value, Put put) throws Exception {

		if(!put.isEmpty() ){
			Table table = HBaseTableUtil.getTable(tableName);
			try {
				if(compareOp == null){
					table.checkAndPut(row, family, qualifier, value, put);
				}else{
					table.checkAndPut(row, family, qualifier, compareOp, value, put);
				}
			}finally{
				HBaseTableUtil.close(table);
			}
		}
	}

}
