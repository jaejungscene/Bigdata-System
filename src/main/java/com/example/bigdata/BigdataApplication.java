package com.example.bigdata;

import com.example.bigdata.util.PropertyFileReader;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class BigdataApplication {

	public static void main(String[] args) throws Exception {
		Properties properties = PropertyFileReader.readPropertyFile("application.properties");
		String fredUrl = properties.getProperty("fred.url");
		System.out.println(fredUrl);

//		SpringApplication.run(BigdataApplication.class, args);
	}

}
