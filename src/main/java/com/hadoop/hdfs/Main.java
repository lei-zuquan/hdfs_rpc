package com.hadoop.hdfs;

import org.apache.hadoop.fs.FileStatus;

import java.io.File;
import java.io.IOException;

/**
 * @Author:
 * @Date: 2020-10-19 12:45
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class Main {

    /**
     * 获取某个文件夹下的所有文件
     */
    public static void getAllFileName(String path) throws Exception {
        System.out.println(path);

        if (!SRC_FILE_PATH.equals(path)){
            String subFilePath = path.substring(SRC_FILE_PATH.length() + 1);
            String fullFilePath = DEST_FILE_PATH + '/' + subFilePath;
            HdfsApi.mkdir(fullFilePath);
        }

        File file = new File(path);
        File[] tempList = file.listFiles();

        for (int i = 0; i < tempList.length; i++) {
            if (tempList[i].isFile()) {
                //System.out.println(tempList[i].getName());
                //System.out.println(tempList[i].getPath());
                String srcFilePath = tempList[i].getPath();
                String subFilePath = srcFilePath.substring(SRC_FILE_PATH.length() + 1);
                String fullFilePath = DEST_FILE_PATH + '/' + subFilePath;
                HdfsApi.copyFileToHDFS(tempList[i].getPath(), fullFilePath);
            }
            if (tempList[i].isDirectory()) {
                getAllFileName(tempList[i].getAbsolutePath());
            }
        }
        return;
    }

    private static String SRC_FILE_PATH = "F:\\ForLei\\法库\\法规分类";
    private static String DEST_FILE_PATH = "/user/root/sqoop/faku";

    public static void main(String[] args) throws IOException {
        try {
            HdfsApi.getFs();
            HdfsApi.copyFileToHDFS("d://test.jpg", "/user/bigdatadev/up_test.jpg");
            FileStatus[] fileStatuses = HdfsApi.listFile("/user/bigdatadev");
            for (FileStatus fileName : fileStatuses){
                System.out.println(fileName.getPath().getName());
            }

            HdfsApi.delete("/user/bigdatadev/up_test.jpg");
            fileStatuses = HdfsApi.listFile("/user/bigdatadev");
            for (FileStatus fileName : fileStatuses){
                System.out.println(fileName.getPath().getName());
            }
//			deletedir(DEST_FILE_PATH,true);
//			try {
//				getAllFileName(SRC_FILE_PATH);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}

        } finally {
            HdfsApi.closeFs();
        }
    }
}
