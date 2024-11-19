package com.handson.searchengine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SearchengineApplication {

	public static void main(String[] args) {
		SpringApplication.run(SearchengineApplication.class, args);
	}

}
