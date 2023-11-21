package com.example.bigdata.processor;

import com.example.bigdata.pojo.EtlColumnPojo;
import com.example.bigdata.pojo.FredColumnPojo;
import com.example.bigdata.util.PropertyFileReader;
import com.example.bigdata.util.US_STATES;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.stream.Collectors;


/**
 * class for
 *  1. reading data from FRED
 *  2. processing data
 *  3. store processed data to HDFS
 */
public class Fred2Hdfs {
    public enum  APITYPE{
        SEARCH("series/search"),
        OBSERVATION("series/observations");

        private String apiType;

        APITYPE(String apiType) {
            this.apiType = apiType;
        }
    }

    public enum FREQUENCY{
        MONTH('M'),
        YEAR('A');

        private char freq;

        FREQUENCY(char freq) {
            this.freq = freq;
        }
    }

    private Properties fredProp = null;
    private ObjectMapper mapper = null;
    private FileSystem hadoopFs = null;


    public Fred2Hdfs() throws Exception{
        this.fredProp = PropertyFileReader.readPropertyFile("application.properties");
        String HADOOP_CONF_DIR = fredProp.getProperty("hadoop.conf.dir");

        this.mapper = new ObjectMapper();

        Configuration conf = new Configuration();
        conf.addResource(new Path("file:///" + HADOOP_CONF_DIR + "/core-site.xml"));
        conf.addResource(new Path("file:///" + HADOOP_CONF_DIR + "/hdfs-site.xml"));

        String namenode = fredProp.getProperty("hdfs.namenode.url");
        this.hadoopFs = FileSystem.get(new URI(namenode), conf);
    }

//    public Object getDataFromURL(){
//        return
//    }


    public List<EtlColumnPojo> getEtlListData(FREQUENCY freq, US_STATES state, String searchText)
    throws Exception{
        String fredUrl = fredProp.getProperty("fred.url");
        String apiKey = fredProp.getProperty("fred.apiKey");
        String fileType = "json";
        String searchUrl = fredUrl + APITYPE.valueOf("SEARCH").apiType
                + "?search_text=" + searchText.replace(' ', '+')
                + state.getValueFullname().replace(' ', '+')
                + "&api_key=" + apiKey + "&file_type=" + fileType;

        System.out.println(searchUrl);

        JsonNode rootNode = mapper.readTree(new URL(searchUrl));
        Thread.sleep(500);
        ArrayNode nodeSeriess = (ArrayNode) rootNode.get("seriess");
        List<FredColumnPojo> listFredData =
                mapper.readValue(nodeSeriess.traverse(), new TypeReference<List<FredColumnPojo>>(){});

        System.out.println(listFredData.size());

        Predicate<FredColumnPojo> predi = (fred) -> (
                fred.getTitle().equals(searchText + state.getValueFullname())
                && fred.getFrequency_short().charAt(0) == freq.freq
                && fred.getSeasonal_adjustment_short().equals("NSA")
        );

        System.out.println(listFredData.stream().filter(predi).toList().size());

        List<EtlColumnPojo> listData = listFredData.stream().filter(predi).flatMap(
            pojo -> {
                String observUrl = fredUrl + APITYPE.valueOf("OBSERVATION").apiType
                        + "?series_id=" + pojo.getId() + "&api_key=" + apiKey
                        + "&file_type=" + fileType;

                System.out.println(observUrl);

                try {
                    JsonNode nodeValue = mapper.readTree(new URL(observUrl));
                    Thread.sleep(500);
                    ArrayNode nodeValueObserv = (ArrayNode) nodeValue.get("observations");
                    List<EtlColumnPojo> listEtlData = mapper.readValue(
                            nodeValueObserv.traverse(),
                            new TypeReference<List<EtlColumnPojo>>() {}
                    );

                    for (EtlColumnPojo valuePojo : listEtlData) {
                        valuePojo.setState(state.getValueFullname());
                        valuePojo.setId(pojo.getId());
                        valuePojo.setTitle(pojo.getTitle().replace(',', '_'));
                        valuePojo.setFrequency_short(pojo.getFrequency_short());
                        valuePojo.setUnits_short(pojo.getUnits_short());
                        valuePojo.setSeasonal_adjustment_short(pojo.getSeasonal_adjustment_short());
                    }

                    return listEtlData.stream();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        ).collect(Collectors.toList());

        return listData;
    }


    public void writeCsv2Hdfs(String filename, List<EtlColumnPojo> nodeList) throws Exception {
        Path hadoopPath = new Path(filename);
        FSDataOutputStream hadoopOutStream = null;
        BufferedWriter bw = null;
        if (nodeList.size() != 0) {
            if (hadoopFs.exists(hadoopPath)) {
                hadoopOutStream = hadoopFs.append(hadoopPath);
                bw = new BufferedWriter(new OutputStreamWriter(hadoopOutStream, StandardCharsets.UTF_8));
            } else {
                hadoopOutStream = hadoopFs.create(hadoopPath, true);
                bw = new BufferedWriter(new OutputStreamWriter(hadoopOutStream, StandardCharsets.UTF_8));
                bw.write(nodeList.get(0).getColumns());
                bw.newLine();
            }

            for (EtlColumnPojo pojo : nodeList) {
                bw.write(pojo.getValues());
                bw.newLine();
            }
            bw.close();
            hadoopOutStream.close();
        }
    }


    public void writeCsv2Local (boolean first, String path, String filename, List<EtlColumnPojo> nodeList) throws Exception {
        CsvMapper csvMapper = new CsvMapper();
        csvMapper.configure(JsonGenerator.Feature.IGNORE_UNKNOWN, true);
        csvMapper.findAndRegisterModules();
        csvMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        CsvSchema schema = csvMapper.schemaFor(EtlColumnPojo.class)
                .withColumnSeparator(',');
        if (first) {
            schema = schema.withHeader();
        } else {
            schema = schema.withoutHeader();
        }
        File outputFile = new File(path + filename);
        OutputStream os = new FileOutputStream(outputFile, true);

        ObjectWriter ow = csvMapper.writer(schema);
        ow.writeValue(os, nodeList);
        os.close();
    }


    public void clearInputFiles(String path, String filename) throws Exception{
        Path hadoopPath = new Path(filename);

        if (hadoopFs.exists(hadoopPath)) {
            hadoopFs.delete(hadoopPath, true);
            System.out.println("Hadoop File System에서 " + hadoopPath.getName()
            + " 파일이 삭제되었습니다.");
        }

        File localPath = new File(path + filename);
        if (localPath.exists()) {
            localPath.delete();
            System.out.println("Local 환경에서 " + localPath.getName()
            + " 파일이 삭제되었습니다.");
        }
    }


    public void closeStream() throws Exception{
        if (hadoopFs != null) {
            hadoopFs.close();
        }
    }
}
