package com.mycompany.app;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

/**
 * Created by ravi on 28/12/15.
 */

public class KafkaSerializer implements Encoder<Object> {

        private static final Logger logger = Logger.getLogger(KafkaSerializer.class);
        // instantiating ObjectMapper is expensive. In real life, prefer injecting the value.
        private static final ObjectMapper objectMapper = new ObjectMapper();

        public KafkaSerializer(VerifiableProperties verifiableProperties) {
        /* This constructor must be present for successful compile. */
        }

        @Override
        public byte[] toBytes(Object object) {
            try {
                return objectMapper.writeValueAsString(object).getBytes();
            } catch (JsonProcessingException e) {
                logger.error(String.format("Json processing failed for object: %s", object.getClass().getName()), e);
            }
            return "".getBytes();
        }
    }