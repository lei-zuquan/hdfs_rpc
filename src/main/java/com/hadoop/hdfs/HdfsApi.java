package com.hadoop.hdfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsApi {
	private static String HDFS_URL = "hdfs://js001.bigdata.com:8020";
	private static FileSystem fs = null;

	/**
	 * 获取FS
	 */
	public static FileSystem getFs() throws IOException {

		if (fs != null) {
			return fs;
		}

		//System.setProperty("hadoop.home.dir", "/opt/cloudera/parcels/CDH-5.16.1-1.cdh5.16.1.p0.3/bin/");
		// 获取配置文件
		Configuration conf = new Configuration();
		conf.set("fs.default.name", HDFS_URL);
		conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		System.setProperty("HADOOP_USER_NAME","bigdatadev");
		//System.setProperty("HADOOP_USER_PASSWORD","bDdex78K");


		if (StringUtils.isBlank(HDFS_URL)) {
			// 返回默认文件系统 如果圿 Hadoop集群下运行，使用此种方法可直接获取默认文件系统
			try {
				fs = FileSystem.get(conf);
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			// 返回指定的文件系绿,如果在本地测试，霿要使用此种方法获取文件系统
			try {
				URI uri = new URI(HDFS_URL.trim());
				fs = FileSystem.get(uri, conf);
			} catch (URISyntaxException | IOException e) {
				e.printStackTrace();
			}
		}

		return fs;
	}

	/**
	 * 关闭hdfs文件系统
	 */
	public static void closeFs() throws IOException {
		if (fs != null) {
			fs.close();
		}
	}

	/**
	 * 判断hdfs文件系统，文件是否存在
	 */
	public static boolean fileExists(String dirName) throws IOException {
		// 获取 FileSystem
		FileSystem fs = getFs();

		return fs.exists(new Path(dirName));
	}

	/**
	 * 读取hdfs文件信息，并打印在控制台
	 */
	public static void readFile(String src) throws IOException {
		// 获取 FileSystem
		FileSystem fs = getFs();
		// 读的路径
		Path readPath = new Path(src);
		FSDataInputStream inStream = null;
		try {
			// 打开输入
			inStream = fs.open(readPath);
			IOUtils.copyBytes(inStream, System.out, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(inStream);
		}
	}

	/**
	 * 上传文件至hdfs文件系统，如果目录不存在，则会进行递归创建
	 */
	public static void copyFileToHDFS(String src, String dst)
			throws IOException {
		// 获取filesystem
		FileSystem fs = getFs();
		// 本地路径
		File inFile = new File(src);
		// 目标路径
		Path outFile = new Path(dst);
		FileInputStream inStream = null;
		FSDataOutputStream outStream = null;
		try {
			// 打开输入
			inStream = new FileInputStream(inFile);
			// 打开输出
			outStream = fs.create(outFile);

			IOUtils.copyBytes(inStream, outStream, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(inStream);
			IOUtils.closeStream(outStream);
		}
	}

	/**
	 * 从hdfs文件系统中下载文件
	 */
	public static void downLoadFromHDFS(String src, String dst)
			throws IOException {
		FileSystem fs = getFs();
		// 设置下载地址和目标地址
		fs.copyToLocalFile(false, new Path(src), new Path(dst), true);
	}

	/**
	 * 对hdfs文件系统中的文件,进行重命名和移动
	 */
	public static void renameMV(String src, String dst) throws IOException {
		FileSystem fs = getFs();
		fs.rename(new Path(src), new Path(dst));
	}

	/**
	 * 对hdfs文件系统中的文件,进行删除
	 */
	public static void delete(String fileName) throws IOException {
		FileSystem fs = getFs();
		fs.deleteOnExit(new Path(fileName));
	}

	/**
	 * 对hdfs文件系统中的文件,获取文件列表
	 */
	public static FileStatus[] listFile(String dirName) throws IOException {
		FileSystem fs = getFs();
		FileStatus[] fileStatuses = fs.listStatus(new Path(dirName));
		// for (FileStatus fileName : fileStatuses) {
		// System.out.println(fileName.getPath().getName());
		// }
		return fileStatuses;
	}

	/**
	 * 查找某个文件在HDFS集群的位置
	 */
	public static BlockLocation[] getFileBlockLocations(String filePath) {
		// 文件路径
		Path path = new Path(filePath);

		// 文件块位置
		BlockLocation[] blkLocations = new BlockLocation[0];
		try {
			// 返回FileSystem对
			FileSystem fs = getFs();
			// 获取文件目录
			FileStatus filestatus = fs.getFileStatus(path);
			// 获取文件块位置列衿
			blkLocations = fs.getFileBlockLocations(filestatus, 0,
					filestatus.getLen());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return blkLocations;
	}

	/**
	 * 对hdfs文件系统中的文件,创建目录; 可以递归创建目录
	 */
	public static void mkdir(String dirName) throws IOException {
		FileSystem fs = getFs();

		if (fs.exists(new Path(dirName))) {
			System.out.println("Directory already exists!");
			//fs.close();
			return;
		}
		fs.mkdirs(new Path(dirName));
	}

	/**
	 * 对hdfs文件系统中的文件,删除目录; recursive，true表示递归删除目录下所有文件，false就只能删除空目录
	 */
	public static void deletedir(String dirName, boolean recursive)
			throws IOException {
		FileSystem fs = getFs();
		// true表示递归删除目录下所有文件
		// false就只能删除空目录
		if (fileExists(dirName)){
			fs.delete(new Path(dirName), recursive);
		}
	}


	private static void apiTest() throws Exception {
		HdfsApi.getFs();

		// boolean fileExist = fileExists("/user/root/spark.txt");
		// System.out.println(fileExist);
		//
		// readFile("/user/root/spark.txt");

		// copyFileToHDFS();

		copyFileToHDFS("d://test.jpg", "/user/root/up_test.jpg");

		// downLoadFromHDFS("/user/root/up_test.jpg", "d://down_test.jpg");

		// renameMV("/user/root/up_test.jpg", "/user/root/up_test_rename.jpg");

		// delete("/user/root/up_test_rename.jpg");

//		FileStatus[] fileStatusArr = listFile("/user/root/sqoop/zxapp_user_behavior/");
//		for (FileStatus fileName : fileStatusArr){
//			System.out.println(fileName.getPath().getName());
//		}

		// BlockLocation[] blockBlockLocations =
		// getFileBlockLocations("/user/root/spark.txt");
		// for (BlockLocation block: blockBlockLocations){
		// System.out.println(block.toString());
		// }

		// mkdir("/user/root/mkdir_Test");

		// deletedir("/user/root/mkdir_Test", true);

		HdfsApi.closeFs();
	}

}
