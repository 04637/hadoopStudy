package hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * HDFS 学习
 *
 * @author: 04637@163.com
 * @date: 2019/5/15
 */
public class HdfsDemo {



    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.137.101:9820");
        System.setProperty("HADOOP_USER_NAME", "root");
        uploadFile(conf);
        listFile(new Path("/"), conf);
        downloadFile(conf);
    }

    /**
     * 创建文件夹
     */
    public static void createFolder(Configuration conf) throws IOException {
        Path path = new Path("/hello2");
        FileSystem fileSystem = path.getFileSystem(conf);
        fileSystem.mkdirs(path);
    }

    /**
     * 递归显示文件
     *
     * @param path 要显示的路径
     */
    public static void listFile(Path path, Configuration conf) {
        try {
            FileSystem fs = FileSystem.get(conf);
            // 传入路径, 表示显示某个路径下的文件夹列表
            // 将给定路径下所有的文件元数据放到一个FileStatus数组中
            // FileStatus 封装了文件和目录的元数据, 包括文件长度, 块大小, 权限等信息
            FileStatus[] fileStatusArray = fs.listStatus(path);
            for (int i = 0; i < fileStatusArray.length; i++) {
                FileStatus fileStatus = fileStatusArray[i];
                // 首先检测当前是否是文件夹, 如果是, 进行递归
                if (fileStatus.isDirectory()) {
                    System.out.println("当前路径是: " + fileStatus.getPath());
                    listFile(fileStatus.getPath(), conf);
                } else {
                    System.out.println("当前路径是: " + fileStatus.getPath());
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }


    public static void uploadFile(Configuration conf){
        try{
            FileSystem fs = FileSystem.get(conf);
            // 定义文件的路径和上传路径
            Path src = new Path("D://test.txt");
            Path dest = new Path("/hello2");
            // 从本地上传文件到服务器上
            fs.copyFromLocalFile(src, dest);
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public static void downloadFile(Configuration conf){
        try{
            FileSystem fs = FileSystem.get(conf);
            // 定义下载文件的路径和本地下载路径
            Path src = new Path("/hello2/test.txt");
            Path dest = new Path("D://download.doc");
            // 从服务器下载文件到本地
            fs.copyToLocalFile(src, dest);
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
