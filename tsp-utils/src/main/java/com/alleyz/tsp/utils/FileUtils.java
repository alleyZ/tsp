package com.alleyz.tsp.utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by alleyz on 2017/5/26.
 *
 */
public class FileUtils {

    /**
     * 文件拷贝
     * @param src 源文件
     * @param dest 目标文件
     * @throws IOException
     */
    public static void copyFile(String src, String dest) throws IOException{
        ByteBuffer buffer = ByteBuffer.allocate(20480);
        try(FileInputStream fis = new FileInputStream(new File(src));
            FileChannel inChannel = fis.getChannel();
            FileOutputStream fos = new FileOutputStream(new File(dest));
            FileChannel outChannel = fos.getChannel();){
            while ( inChannel.read(buffer) != -1) {
                buffer.flip();
                outChannel.write(buffer);
                buffer.clear();
            }
        }
    }

    public static void copyFile(InputStream is, String dest) throws IOException{
        byte[] bytes = new byte[4096];
        int read ;
        try(FileOutputStream fos = new FileOutputStream(dest)){
            while ((read = is.read(bytes)) != -1){
                fos.write(bytes, 0, read);
            }
            fos.flush();
        }
    }
}