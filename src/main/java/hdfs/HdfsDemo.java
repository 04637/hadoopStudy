package hdfs;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * @description: HDFS 学习
 * @author: 04637@163.com
 * @date: 2019/5/15
 */
public class HdfsDemo {
    public static void main(String[] args) {
        uploadFile();
    }

    /**
     * 创建文件夹
     */
    public static void createFolder() {
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path("/cloud");
            fs.mkdirs(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 递归显示文件
     *
     * @param path 要显示的路径
     */
    public static void listFile(Path path) {
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(conf);
            // 传入路径, 表示显示某个路径下的文件夹列表
            // 将给定路径下所有的文件元数据放到一个FileStatus数组中
            // FileStatus 封装了文件和目录的元数据, 包括文件长度, 块大小, 权限等信息
            FileStatus[] fileStatusArray = fs.listStatus(path);
            for (int i = 0; i < fileStatusArray.length; i++) {
                FileStatus fileStatus = fileStatusArray[i];
                // 首先检测当前是否是文件夹, 如果是, 进行递归
                if (fileStatus.isDir()) {
                    System.out.println("当前路径是: " + fileStatus.getPath());
                    listFile(path);
                } else {
                    System.out.println("当前路径是: " + fileStatus.getPath());
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }


    public static void uploadFile(){
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.204.181:9000");
        //通过这种方式设置java客户端身份
        System.setProperty("HADOOP_USER_NAME", "root");
        try{
            FileSystem fs = FileSystem.get(conf);
            // 定义文件的路径和上传路径
            Path src = new Path("D://test.txt");
            Path dest = new Path("cloud/upload.doc");
            // 从本地上传文件到服务器上
            fs.copyFromLocalFile(src, dest);
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public static void downloadFile(){
        Configuration conf = new Configuration();
        try{
            FileSystem fs = FileSystem.get(conf);
            // 定义下载文件的路径和本地下载路径
            Path src = new Path("/cloud/download.doc");
            Path dest = new Path("e://download.doc");
            // 从服务器下载文件到本地
            fs.copyToLocalFile(src, dest);
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
