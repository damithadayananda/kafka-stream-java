package com.example.demo;

import com.example.demo.component.StreamProcessorComponent;
import com.example.demo.controller.StreamProcessorController;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;

@SpringBootApplication
public class GenericMaterializerApplication  implements CommandLineRunner {
	private static final Logger LOG= LogManager.getLogger(StreamProcessorComponent.class);

	@Autowired
	private StreamProcessorController streamProcessorController;

	public static void main(String[] args) {
		SpringApplication.run(GenericMaterializerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		//streamProcessorController.streamProcessorDemoController();
	}
	@PostConstruct
	public void start(){
		streamProcessorController.streamProcessorDemoController();
	}
}
