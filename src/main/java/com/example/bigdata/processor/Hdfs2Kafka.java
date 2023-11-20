package com.example.bigdata.processor;

import com.example.bigdata.util.PropertyFileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class Hdfs2Kafka {
    private Properties systemProp = null;
    private FileSystem hadoopFs = null;

    public Hdfs2Kafka() throws Exception {
        systemProp = PropertyFileReader.readPropertyFile("application.properties");
        String HADOOP_CONF_DIR = systemProp.getProperty("hadoop.conf.dir");

        Configuration conf = new Configuration();
        conf.addResource(new Path("file:///" + HADOOP_CONF_DIR + "/core-site.xml"));
        conf.addResource(new Path("file:///" + HADOOP_CONF_DIR + "/hdfs-site.xmI"));

        String namenode = systemProp.getProperty("hdfs.namenode.url");
        hadoopFs = FileSystem.get(new URI(namenode), conf);
    }


    public List<String> readHdFile(String filename) throws Exception {
        Path path = new Path(filename);
        List<String> lines = new ArrayList<>();

        if (!hadoopFs.exists(path)) {
            System.out.println("파일이 존재하지 않습니다.");
            return null;
        }

        FSDataInputStream input = hadoopFs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(input));

        while (br.ready()) {
            String[] line = br.readLine().split(",");
            lines.add(line[2] + "," + line[3] + "," + line[4] + "," + line[5]
                    + line[6] + "," + line[7] + "," + line[8] + "," + line[9]);
        }

        br.close();
        input.close();
        return lines;
    }

    public void getHdFilesInfo(String filename) throws Exception {
        Path path = new Path(filename);
        FileStatus fStatus = hadoopFs.getFileStatus(path);
        if (fStatus.isFile()) {
            System.out.println("파일 블럭 사이즈 : " + fStatus.getBlockSize());
            System.out.println("파일 Group : " + fStatus.getGroup());
            System.out.println("파일 Owner : " + fStatus.getOwner());
            System.out.println("파일 길이 : " + fStatus.getLen());
        } else {
            System.out.println("파일이 아닙니다!!");
        }
    }

    public void sendLines2Kafka(String topic, String line){
        System.out.println(line);
    }

    public void closeStream() throws Exception {
        if (hadoopFs != null) {
            hadoopFs.close();
        }
    }
}