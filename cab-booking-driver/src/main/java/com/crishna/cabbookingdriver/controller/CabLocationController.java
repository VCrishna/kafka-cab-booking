package com.crishna.cabbookingdriver.controller;

import com.crishna.cabbookingdriver.service.CabLocationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/location")
public class CabLocationController {

    @Autowired
    private CabLocationService cabLocationService;

    @RequestMapping(value = "/update",method = RequestMethod.PUT)
    public ResponseEntity updateLocation() throws InterruptedException {
        int range = 100;
        while(range > 0) {
            cabLocationService.update(Math.random()+" , "+Math.random());
            Thread.sleep(1000);
            range--;
        }
        return new ResponseEntity<>(Map.of("Message","Location Updated"), HttpStatus.OK);
    }
}
