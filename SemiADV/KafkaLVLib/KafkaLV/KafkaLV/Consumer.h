#pragma once
#include "librdkafka/rdkafka.h"
#include <vector>
#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include "KafkaLVDataDef.h"


class KConsumer
{
public:
	KConsumer();
	virtual ~KConsumer();

public:
	//Public Methods
	long Initialize(std::string kafkaBroker, std::string topic, int32_t partition);
	long Initialize(std::string kafkaBroker);
	long ConsumeFromBeginning(int64_t maxEvents, int64_t* numEvents);
	//long ConsumeFromEnd(int64_t maxEvents, int64_t* numEvents);
	long ConsumeFromEnd(std::vector<kafkaEvent> &events, int32_t count);
	long Consume(int64_t maxEvents, int64_t* numEvents, int64_t offset);
	void GetData(std::vector<kafkaEvent>** events);
	long GetMinMaxOffsets(int64_t* min, int64_t* max);
	long Seek(int64_t offset);
	void ExitLoop(void);
private:
	long ProcessMessage(rd_kafka_message_t* message, kafkaEvent& event);
	long CreatePartition(int64_t offset);
	void DestroyPartition(void);


private:
	std::string m_kafkaBroker;
	std::string m_kafkaTopic;
	rd_kafka_t *m_consumer;
	std::string m_groupID;
	int m_partition;
	int64_t m_messageCount;
	int64_t m_messageBytes;
	std::vector<kafkaEvent> m_consumerEvents;
	std::vector<std::string> m_errorLog;
	rd_kafka_topic_partition_list_t* m_topicPartition;
	std::string m_errorString;
	uint32_t s_run;
};
/*
class ExampleEventCb : public RdKafka::EventCb 
{
	public:
		void event_cb(RdKafka::Event &event) 
		{

			print_time();

			switch (event.type())
			{
				case RdKafka::Event::EVENT_ERROR:
					if (event.fatal()) {
						std::cerr << "FATAL ";
						//run = 0;
					}
					std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
						event.str() << std::endl;
					break;

				case RdKafka::Event::EVENT_STATS:
					std::cerr << "\"STATS\": " << event.str() << std::endl;
					break;

				case RdKafka::Event::EVENT_LOG:
					fprintf(stderr, "LOG-%i-%s: %s\n",
						event.severity(), event.fac().c_str(), event.str().c_str());
					break;

				case RdKafka::Event::EVENT_THROTTLE:
					std::cerr << "THROTTLED: " << event.throttle_time() << "ms by " <<
						event.broker_name() << " id " << (int)event.broker_id() << std::endl;
					break;

				default:
					std::cerr << "EVENT " << event.type() <<
						" (" << RdKafka::err2str(event.err()) << "): " <<
						event.str() << std::endl;
					break;
			}
		}
		static void print_time() {
#ifndef _WIN32
			struct timeval tv;
			char buf[64];
			gettimeofday(&tv, NULL);
			strftime(buf, sizeof(buf) - 1, "%Y-%m-%d %H:%M:%S", localtime(&tv.tv_sec));
			fprintf(stderr, "%s.%03d: ", buf, (int)(tv.tv_usec / 1000));
#else
			SYSTEMTIME lt = { 0 };
			GetLocalTime(&lt);
			// %Y-%m-%d %H:%M:%S.xxx:
			fprintf(stderr, "%04d-%02d-%02d %02d:%02d:%02d.%03d: ",
				lt.wYear, lt.wMonth, lt.wDay,
				lt.wHour, lt.wMinute, lt.wSecond, lt.wMilliseconds);
#endif
		}

};
*/