package com.example.demo.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
@Component
public class StreamProcessorComponent {

        private static final Logger LOG = LogManager.getLogger(StreamProcessorComponent.class);

        @Value("${materializer.kafka.group_id}")
        private String groupId;
        @Value("${materializer.kafka.brokers}")
        private String brokers;
        @Value("${materializer.kafka.commit_interval}")
        private Integer interval;
        @Value("${materializer.kafka.commit_offset}")
        private String offset;

        @Value("${materializer.schema_registry.url}")
        private String schemaRegistryUrl;

        @Value("${materializer.topic.base}")
        private String baseTopic;
        @Value("${materializer.topic.materialized}")
        private String materializedTopic;

        @Value("${materializer.event.name.MyEvent1}")
        private String MyEvent1;
        @Value("${materializer.event.name.MyEvent2}")
        private String MyEvent3;
        @Value("${materializer.event.name.MyEvent3}")
        private String MyEvent2;
        @Value("${materializer.event.name.MyEvent4}")
        private String materializedType;

        public void StreamProcessor() {
            logProperties();

            Properties streamsConfiguration = new Properties();
            streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, groupId);
            streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
            streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, interval);
            streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset);
            streamsConfiguration.put("schema.registry.url", schemaRegistryUrl);


            final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaRegistryUrl);

            final Serde<String> stringSerde = new Serdes.StringSerde();

            final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
            valueGenericAvroSerde.configure(serdeConfig, false);

            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, GenericRecord> stream = builder.stream(baseTopic, Consumed.with(stringSerde, valueGenericAvroSerde));


            KStream<String, GenericRecord> tripStream[] = stream.branch(
                    (k, v) -> (v.getSchema().getName().equals(MyEvent1)),
                    (k, v) -> (v.getSchema().getName().equals(MyEvent3)),
                    (k, v) -> (v.getSchema().getName().equals(MyEvent2))
            );

            KStream<String, MyEvent1> MyEvent1KStream = tripStream[0].mapValues(
                    (v) -> {
                        LOG.info("message received to trip Created stream");
                        ObjectMapper objectMapper = new ObjectMapper();
                        try {
                            MyEvent1 tc = objectMapper.readValue(v.toString(), MyEvent1.class);
                            return tc;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return null;
                    }
            );

            KStream<String, MyEvent2> MyEvent2KStream = tripStream[2].mapValues(
                    (v) -> {
                        LOG.info("message received to trip Cancelled stream");
                        ObjectMapper objectMapper = new ObjectMapper();
                        try {
                            MyEvent2 tc = objectMapper.readValue(v.toString(), MyEvent2.class);
                            return tc;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return null;
                    }
            );

            KStream<String, MyEvent3> MyEvent3KStream = tripStream[1].mapValues(
                    (v) -> {
                        LOG.info("message received to trip Completed stream");
                        ObjectMapper objectMapper = new ObjectMapper();
                        try {
                            MyEvent3 tc = objectMapper.readValue(v.toString(), MyEvent3.class);
                            return tc;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return null;
                    }
            );


            KTable<String, MyEvent1> MyEvent1Table = MyEvent1KStream.groupByKey().reduce(
                    (k, v) -> {
                        LOG.info("trip created message received to created Table");
                        return v;
                    }
            );

            KTable<String, MyEvent2> MyEvent2Table = MyEvent2KStream.groupByKey().reduce(
                    (k, v) -> {
                        LOG.info("trip cancelled message received to canceled Table");
                        return v;
                    }
            );

            KTable<String, MyEvent3> MyEvent3Table = MyEvent3KStream.groupByKey().reduce(
                    (k, v) -> {
                        LOG.info("trip completed message received to completed table");
                        return v;
                    }
            );


            KTable<String, MyMaterialized> joinedTable1 = MyEvent1Table.leftJoin(MyEvent3Table, (tcre, tcom) -> {
                LOG.info("join table one hit");
                long millis = System.currentTimeMillis();
                UUID uuid = UUID.randomUUID();
                LOG.info("MyEvent1 event:" + tcre.toString());
                String MyEvent3 = tcom == null ? "" : tcom.toString();
                LOG.info("MyEvent3 event:" + MyEvent3);

                Body body = Body.newBuilder()
                        .setMyEvent1(setMyEvent1(tcre))
                        .setMyEvent2(setMyEvent2(null))
                        .setMyEvent3(setMyEvent3(tcom))
                        .build();
                MyMaterialized m = MyMaterialized.newBuilder()
                        .setCreatedAt(millis)
                        .setExpiry(1234)
                        .setId(uuid.toString())
                        .setType(materializedType)
                        .setVersion(1)
                        .setBody(body)
                        .setTraceInfo(setTraceInfo())
                        .build();
                return m;
            });


            KTable<String, MyMaterialized> joinedTable2 = joinedTable1.leftJoin(MyEvent2Table, (mat, tcan) -> {
                LOG.info("joined table 2 hit");
                String MyEvent2 = tcan == null ? "" : tcan.toString();
                LOG.info("trip cancelled event:" + MyEvent2);
                mat.getBody().setMyEvent2(setMyEvent2(tcan));
                LOG.info("materialized event:" + mat.toString());
                return mat;
            });


            joinedTable2.to(materializedTopic);

            final Topology topology = builder.build();

            LOG.info("Topology:" + topology.describe());

            final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);

            streams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


        }

        private materialized.MyEvent3.MyEvent3 setMyEvent3(MyEvent3 tcom) {
            MyEvent3.flags flags = MyEvent3.flags.newBuilder()
                    .setItc(tcom == null ? false : tcom.getBody().getTrip().getFlags().getItc())
                    .build();
            List<payment_record> payment_records_list = new ArrayList<>();
            if (tcom != null) {
                int i = 0;
                for (MyEvent3.Payment l : tcom.getBody().getTrip().getPayment()) {
                    MyEvent3.payment_record payment_record = MyEvent3.payment_record.newBuilder()
                            .setMethod(tcom.getBody().getTrip().getPayment().get(i).getMethod())
                            .setAmount(tcom.getBody().getTrip().getPayment().get(i).getAmount())
                            .build();
                    payment_records_list.add(payment_record);
                    i++;
                }
            }

            MyEvent3.corporate corporate = MyEvent3.corporate.newBuilder()
                    .setCompanyId(tcom == null ? 0 : tcom.getBody().getTrip().getCorporate().getCompanyId())
                    .setDepartmentId(tcom == null ? 0 : tcom.getBody().getTrip().getCorporate().getDepartmentId())
                    .build();

            materialized.MyEvent3.trip trip = materialized.MyEvent3.trip.newBuilder()
                    .setId(tcom == null ? 0 : tcom.getBody().getTrip().getId())
                    .setBookedBy(tcom == null ? 0 : tcom.getBody().getTrip().getBookedBy())
                    .setDistance(tcom == null ? 0 : tcom.getBody().getTrip().getDistance())
                    .setTripCost(tcom == null ? 0 : tcom.getBody().getTrip().getTripCost())
                    .setFlags(flags)
                    .setPayment(payment_records_list)
                    .setDiscount(tcom == null ? 0 : tcom.getBody().getTrip().getDiscount())
                    .setPromoCode(tcom == null ? "" : tcom.getBody().getTrip().getPromoCode())
                    .setCorporate(corporate)
                    .build();
            materialized.MyEvent3.MyEvent3 MyEvent3 = materialized.MyEvent3.MyEvent3.newBuilder()
                    .setTrip(trip)
                    .setDriverId(tcom == null ? 0 : tcom.getBody().getDriverId())
                    .setPassengerId(tcom == null ? 0 : tcom.getBody().getPassengerId())
                    .build();
            return MyEvent3;
        }

        private materialized.MyEvent2.MyEvent2 setMyEvent2(MyEvent2 tcan) {
            materialized.MyEvent2.MyEvent2 MyEvent2 = materialized.MyEvent2.MyEvent2.newBuilder()
                    .setTripId(tcan == null ? 0 : tcan.getBody().getTripId())
                    .setCancelledFrom(tcan == null ? 0 : tcan.getBody().getCancelledFrom())
                    .setCancelledBy(tcan == null ? 0 : tcan.getBody().getCancelledBy())
                    .setReasonId(tcan == null ? 0 : tcan.getBody().getReasonId())
                    .setNote(tcan == null ? "" : tcan.getBody().getNote())
                    .setCancelType(tcan == null ? 0 : tcan.getBody().getCancelType())
                    .build();
            return MyEvent2;
        }

        private materialized.MyEvent1.MyEvent1 setMyEvent1(MyEvent1 tcre) {
            materialized.MyEvent1.corporate_two corporate_two = materialized.MyEvent1.corporate_two.newBuilder()
                    .setId(tcre.getBody().getCorporate().getId())
                    .setDepId(tcre.getBody().getCorporate().getDepId())
                    .build();

            materialized.MyEvent1.passenger passenger = materialized.MyEvent1.passenger.newBuilder()
                    .setId(tcre.getBody().getPassenger().getId())
                    .build();
            materialized.MyEvent1.driver driver = com.pickme.events.finance.materialized.MyEvent1.driver.newBuilder()
                    .setId(tcre.getBody().getDriver().getId())
                    .build();
            materialized.MyEvent1.payment payment = materialized.MyEvent1.payment.newBuilder()
                    .setPrimaryMethod(tcre.getBody().getPayment().getPrimaryMethod())
                    .setSecondaryMethod(tcre.getBody().getPayment().getSecondaryMethod())
                    .build();
            materialized.MyEvent1.surge surge = materialized.MyEvent1.surge.newBuilder()
                    .setRegionId(tcre.getBody().getSurge().getRegionId())
                    .setValue(tcre.getBody().getSurge().getValue())
                    .build();
            materialized.MyEvent1.promotion promotion = materialized.MyEvent1.promotion.newBuilder()
                    .setCode(tcre.getBody().getPromotion().getCode())
                    .build();
            materialized.MyEvent1.fare_details fare_details = materialized.MyEvent1.fare_details.newBuilder()
                    .setFareType(tcre.getBody().getFareDetails().getFareType())
                    .setMinKm(tcre.getBody().getFareDetails().getMinKm())
                    .setMinFare(tcre.getBody().getFareDetails().getMinFare())
                    .setAdditionalKmFare(tcre.getBody().getFareDetails().getAdditionalKmFare())
                    .setWaitingTimeFare(tcre.getBody().getFareDetails().getWaitingTimeFare())
                    .setFreeWaitingTime(tcre.getBody().getFareDetails().getFreeWaitingTime())
                    .setNightFare(tcre.getBody().getFareDetails().getNightFare())
                    .setRideHours(tcre.getBody().getFareDetails().getRideHours())
                    .setExtraRideFare(tcre.getBody().getFareDetails().getExtraRideFare())
                    .setDriverBata(tcre.getBody().getFareDetails().getDriverBata())
                    .setTripType(tcre.getBody().getFareDetails().getTripType())
                    .build();
            materialized.MyEvent1.MyEvent1 MyEvent1 = materialized.MyEvent1.MyEvent1.newBuilder()
                    .setModule(tcre.getBody().getModule())
                    .setServiceGroupCode(tcre.getBody().getServiceGroupCode())
                    .setBookedBy(tcre.getBody().getBookedBy())
                    .setTripId(tcre.getBody().getTripId())
                    .setVehicleType(tcre.getBody().getVehicleType())
                    .setPreBooking(tcre.getBody().getPreBooking())
                    .setCorporateTwo(corporate_two)
                    .setPassenger(passenger)
                    .setDriver(driver)
                    .setPayment(payment)
                    .setSurge(surge)
                    .setPromotion(promotion)
                    .setFareDetails(fare_details)
                    .build();
            return MyEvent1;
        }

        private trace_info setTraceInfo() {
            trace_id traceId = trace_id.newBuilder()
                    .setHigh(123456)
                    .setLow(654321)
                    .build();
            trace_info traceInfo = trace_info.newBuilder()
                    .setTraceId(traceId)
                    .setParentId(123456)
                    .setSampled(true)
                    .setSpanId(123456)
                    .build();
            return traceInfo;
        }
        private void logProperties(){
            LOG.info("======================property list log========================");
            LOG.info("materializer.kafka.group_id:"+groupId);
            LOG.info("materializer.kafka.brokers:"+brokers);
            LOG.info("materializer.kafka.commit_interval:"+interval);
            LOG.info("materializer.kafka.commit_offset:"+offset);
            LOG.info("materializer.schema_registry.url:"+schemaRegistryUrl);
            LOG.info("materializer.topic.base:"+baseTopic);
            LOG.info("materializer.topic.materialized:"+materializedTopic);
            LOG.info("materializer.event.name.MyEvent1:"+MyEvent1);
            LOG.info("materializer.event.name.MyEvent3:"+MyEvent3);
            LOG.info("materializer.event.name.MyEvent2:"+MyEvent2);
            LOG.info("======================property list done=======================");

        }
    }

