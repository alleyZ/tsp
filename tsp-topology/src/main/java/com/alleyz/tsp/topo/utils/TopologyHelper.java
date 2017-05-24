package com.alleyz.tsp.topo.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Created by alleyz on 2017/5/23.
 *
 */
public class TopologyHelper {
    /**
     * 追加文件内容到hdfs（如文件不存在 则创建）
     * @param hdfsFile 文件路径
     * @param content 追加内容
     * @param conf 配置
     * @throws IOException 异常
     */
    public static void appendHDFS(String hdfsFile, String content, Configuration conf) throws IOException{
        Path path = new Path(hdfsFile);
        try(FileSystem fs = FileSystem.get(conf)){
            FSDataOutputStream fos;
            // 为解决hdfs只允许一个client对文件操作限制，此处循环判断文件是否存在，不存在则让其创建
            // 若创建出现异常，让其持续性创建，直到创建成功，然后获取文件流 进行文件追加
            int tryCount = 5;
            while (true){
                if(fs.exists(path)){
                    fos = fs.append(path);
                    break;
                }
                if(tryCount > 0){
                    try{
                        Thread.sleep(1000);
                        if(!fs.exists(path)){
                            fs.create(path);
                        }
                    }catch (Exception e){}
                    tryCount --;
                }

            }
            ByteArrayInputStream is = new ByteArrayInputStream(content.getBytes());
            IOUtils.copyBytes(is, fos, 4096, false);
        }
    }
}
