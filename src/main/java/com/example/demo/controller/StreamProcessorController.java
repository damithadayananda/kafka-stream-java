package com.example.demo.controller;

import com.example.demo.service.StreamProcessorDemoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

@Controller
public class StreamProcessorController {
    @Autowired
    private StreamProcessorDemoService streamProcessorDemoService;

    public void streamProcessorDemoController(){
        streamProcessorDemoService.StreamProcessorDemoService();
    }
}
