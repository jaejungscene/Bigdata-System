package com.example.bigdata.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyFileReader {
    private static Properties prop = new Properties();

    public static Properties readPropertyFile(String file) throws Exception{
        // 주어진 파일 이름으로부터 속성값을 읽어와 Property 객체에 저장합니다.
        if (prop.isEmpty()) {
            InputStream input = PropertyFileReader.class.getClassLoader().getResourceAsStream(file);
            try {
                prop.load(input);
            } catch (IOException exception) {
                throw exception;
            } finally {
                if (input != null) {
                    input.close();
                }
            }
        }
        return prop;
    }
}
