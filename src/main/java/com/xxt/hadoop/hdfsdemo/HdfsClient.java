package com.xxt.hadoop.hdfsdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author xuxiaotong
 * @description
 */
public class HdfsClient {

    @Test
    public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        // configuration.set("fs.defaultFS", "hdfs://hadoop102:9000"); // core-site.xml 里的配置
        // FileSystem fs = FileSystem.get(configuration);

        FileSystem fs = FileSystem.get(new URI("hdfs://host101:9000"), configuration, "root");

        // 2 创建目录
        fs.mkdirs(new Path("/1108/daxian/banzhang"));

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://host101:9000"), configuration, "root");

        // 2 上传文件
        fs.copyFromLocalFile(new Path("D:/developer/testdir/banzhang.txt"), new Path("/banzhang.txt"));

        // 3 关闭资源
        fs.close();

        System.out.println("over");
    }

    @Test
    public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://host101:9000"), configuration, "root");

        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("/banzhang.txt"), new Path("e:/banhua.txt"), true);

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testDelete() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://host101:9000"), configuration, "root");

        // 2 执行删除
        fs.delete(new Path("/1108/"), true); //设置递归删除

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testRename() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://host101:9000"), configuration, "root");
        // 2 修改文件名称
        fs.rename(new Path("/banzhang.txt"), new Path("/aaa.txt"));

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException{

        // 1获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://host101:9000"), configuration, "root");
        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles
                = fs.listFiles(new Path("/"), true);


        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();

            // 输出详情
            // 文件名称
            System.out.println("-----------------------------------");
            System.out.println("file->" + status.getPath().getName());
            // 长度
            System.out.println("----->" + status.getLen());
            // 权限
            System.out.println("----->" + status.getPermission());
            // 分组
            System.out.println("----->" + status.getGroup());

            System.out.println("获取存储的块信息");
            // 获取存储的块信息
            BlockLocation[] blockLocations
                    = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {

                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                System.out.println("获取块存储的主机节点");

                for (String host : hosts) {
                    System.out.println(host);
                }
            }

        }

        // 3 关闭资源
        fs.close();
    }



}


