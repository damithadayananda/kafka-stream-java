package com.example.demo.service;

import com.example.demo.component.StreamProcessorComponent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class StreamProcessorDemoService {
    @Autowired
    private StreamProcessorComponent streamProcessorComponent;

    public void StreamProcessorDemoService() {
        streamProcessorComponent.StreamProcessor();
    }
}
