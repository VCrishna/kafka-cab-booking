package com.crishna.cabbookingdriver;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;

import java.util.Date;

@SpringBootApplication
public class CabBookingDriverApplication {
	@Autowired
	private Environment env;

	private Logger logger = LoggerFactory.getLogger(CabBookingDriverApplication.class);

	@PostConstruct
	public void init() {
		String serverPort = env.getProperty("server.port");
		logger.info("CabBookingDriverApplication is started at " + new Date()+" !");
		logger.info("CabBookingDriverApplication is started at " + (serverPort != null ? serverPort : "unknown port"));
	}

	public static void main(String[] args) {
		SpringApplication.run(CabBookingDriverApplication.class, args);
	}

}
