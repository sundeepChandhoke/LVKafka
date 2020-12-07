#pragma once
#include "librdkafka/rdkafka.h"
#include <vector>
#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
//#include <csignal>
#include <cstring>
#include "KafkaLVDataDef.h"

class KProducer
{
public:
	 //Constructor
	 KProducer();
	 virtual ~KProducer();

public:
	//Public Methods
	long Initialize(std::string kafkaBroker);
	long Initialize(std::string kafkaBroker, std::string topic, double bufferingTimeMS);
	long SendEvents(int32_t partition, std::vector<kafkaEvent>& events);

private:
	std::string m_kafkaBroker;
	std::string m_kafkaTopic;
	rd_kafka_t* m_producer;
	std::string m_lingerMS;
	std::string m_errorString;
	static std::vector<std::string> m_errorLog;

private:
	static void dr_msg_cb(rd_kafka_t* rk, const rd_kafka_message_t* rkmessage, void* opaque);

};
