package com.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;

public class HdfsTest {

	public static void main(String[] args) {
		try {
			FileStatus[] fileStatus = HdfsApi.listFile("/user/root/sqoop/zxapp_user_behavior/");
			for (FileStatus file : fileStatus) {
				System.out.println(file.getPath().getName());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
