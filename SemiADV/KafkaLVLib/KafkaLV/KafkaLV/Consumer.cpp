#include "Consumer.h"
/*
* librdkafka - Apache Kafka C library
*
* Copyright (c) 2014, Magnus Edenhill
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*
* 1. Redistributions of source code must retain the above copyright notice,
*    this list of conditions and the following disclaimer.
* 2. Redistributions in binary form must reproduce the above copyright notice,
*    this list of conditions and the following disclaimer in the documentation
*    and/or other materials provided with the distribution.
*
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
* AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
* IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
* ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
* LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
* CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
* SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
* INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
* CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
* ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
* POSSIBILITY OF SUCH DAMAGE.
*/

/**
* Apache Kafka consumer & producer example programs
* using the Kafka driver from librdkafka
* (https://github.com/edenhill/librdkafka)
*/



#ifndef _WIN32
#include <sys/time.h>
#else
#include <windows.h> /* for GetLocalTime */
#endif

/*
#ifdef _MSC_VER
#include "win32/wingetopt.h"
#elif _AIX
#include <unistd.h>
#else
#include <getopt.h>
#include <unistd.h>
#endif
*/
//------------------------------------------------------------------------------------
// Construction/Destruction
//------------------------------------------------------------------------------------
KConsumer::KConsumer()
{
	s_run = 1;
	m_kafkaTopic.assign("qamData");
	m_consumer = NULL;
	m_groupID.assign("semi_adv");
	m_partition = 0;
	m_messageBytes = 0;
	m_messageCount = 0;
	m_topicPartition = NULL;
}


KConsumer::~KConsumer()
{
	DestroyPartition();
	if (m_consumer)
	{
		rd_kafka_consumer_close(m_consumer);
		rd_kafka_destroy(m_consumer);
	}
}
//-------------------------------------------------------------------------------
// Public Methods
//-------------------------------------------------------------------------------
 long KConsumer::Initialize(std::string kafkaBroker, std::string topic, int32_t partition)
{
	m_kafkaTopic.assign(topic);
	m_partition = partition;
	return Initialize(kafkaBroker);
}
 //-------------------------------------------------------------------------------
 //++
 // Initialize Consumer
 //++
 long KConsumer::Initialize(std::string kafkaBroker)
{
	char errstr[512];

	m_kafkaBroker.assign(kafkaBroker);

	//https://docs.confluent.io/5.5.0/clients/librdkafka/md_CONFIGURATION.html
	rd_kafka_conf_t *conf = rd_kafka_conf_new();
	//RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	/* Set bootstrap broker(s) as a comma-separated list of
		 * host or host:port (default port 9092).
		 * librdkafka will use the bootstrap brokers to acquire the full
		 * set of brokers from the cluster. */
	if (rd_kafka_conf_set(conf, "bootstrap.servers", kafkaBroker.c_str(),
		errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		m_errorString.assign(errstr);
		rd_kafka_conf_destroy(conf);
		return FAIL;
	}
	/* Set the consumer group id.
		 * All consumers sharing the same group id will join the same
		 * group, and the subscribed topic' partitions will be assigned
		 * according to the partition.assignment.strategy
		 * (consumer config property) to the consumers in the group. */
	if (rd_kafka_conf_set(conf, "group.id", m_groupID.c_str(),
		errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		m_errorString.assign(errstr);
		rd_kafka_conf_destroy(conf);
		return FAIL;
	}
	//	Emit RD_KAFKA_RESP_ERR__PARTITION_EOF event whenever the consumer reaches the end of a partition.
	if (rd_kafka_conf_set(conf, "enable.partition.eof", "true",
		errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		m_errorString.assign(errstr);
		rd_kafka_conf_destroy(conf);
		return FAIL;
	}
	// Manually commit offsets.
	if (rd_kafka_conf_set(conf, "enable.auto.commit", "false",
		errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		m_errorString.assign(errstr);
		rd_kafka_conf_destroy(conf);
		return FAIL;
	}

	/* If there is no previously committed offset for a partition
			 * the auto.offset.reset strategy will be used to decide where
			 * in the partition to start fetching messages.
			 * By setting this to earliest the consumer will read all messages
			 * in the partition if there was no previously committed offset. */
	if (rd_kafka_conf_set(conf, "auto.offset.reset", "earliest",
		errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		m_errorString.assign(errstr);
		rd_kafka_conf_destroy(conf);
		return FAIL;
	}
	

	//ExampleEventCb ex_event_cb;
	//if (conf->set("event_cb", &ex_event_cb, errstr) != RdKafka::Conf::CONF_OK)
	//{
	//	std::cerr << errstr << std::endl;
	//	delete conf;
	//	return -1;
	//}

	/*
	* Consumer mode
	*/

	/*
		 * Create consumer instance.
		 *
		 * NOTE: rd_kafka_new() takes ownership of the conf object
		 *       and the application must not reference it again after
		 *       this call.
		 */
	m_consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
	if (!m_consumer) {
		m_errorString.assign(errstr);
		return NO_INTERFACE;
	}

	conf = NULL; /* Configuration object is now owned, and freed,
				  * by the rd_kafka_t instance. */

	
	
	return OK;
}
 void KConsumer::ExitLoop(void)
 {
	 s_run = 0;
 }
 //------------------------------------------------------------------------------------------------
 //++
 // Consume .. blocks till it reaches end of partition or from the beginning of the log for the topic
 //++
 long KConsumer::ConsumeFromBeginning(int64_t maxEvents, int64_t* numEvents)
 {
	 return Consume(maxEvents, numEvents, RD_KAFKA_OFFSET_BEGINNING);

 }
 //------------------------------------------------------------------------------------------------
 //++
 // ConsumeFromEnd .. blocks till it gets a number of messages specified in count and then returns them to a
 // loop executing in the higher level application
 //++
 long KConsumer::ConsumeFromEnd(std::vector<kafkaEvent>& events, int32_t count)
 {

	 long err = 0;
	 if (m_topicPartition == NULL)
	 {
		 // Create the partition for this topic with offset (seek can only be called on actively fetched partitions)
		 // https://github.com/confluentinc/confluent-kafka-go/issues/121
		 err = CreatePartition(RD_KAFKA_OFFSET_END);
	 }

	 /*
	 * Consume messages
	 */
	 rd_kafka_poll_set_consumer(m_consumer);
	 m_errorLog.clear();
	 int32_t numMsgs = 0;
	 while (s_run) {
		 rd_kafka_message_t* msg = rd_kafka_consumer_poll(m_consumer, 100);
		 if (!msg)
		 {
			 continue;	/* Timeout: no message within 100ms,
						 *  try again. This short timeout allows
						 *  checking for `run` at frequent intervals.
						 */
		 }
		 kafkaEvent event;
		 err = ProcessMessage(msg, event);
		 if (err == OK)
		 {
			 events.push_back(event);
			 numMsgs++;
		 }
		 else if (err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
		 {
			 //This is normal .. so suppress the error and continue
			 err = OK;
		 }
		 rd_kafka_message_destroy(msg);
		 if (numMsgs == count || err != OK)
		 {
			 break;
		 }
	 }
	 if (err == S_OK)
	 {
		 err = rd_kafka_commit(m_consumer, NULL, 0 /*sync*/);
	 }

	 return err;
 }
 //------------------------------------------------------------------------------------------------
 //++
 // Consume .. blocks till it reaches end of partition or maxEvents have been retrieved
 // Consumer will try to use the last consumed offset as the starting offset and fetch sequentially. 
 // The last consumed offset can be manually set through seek(TopicPartition, long) or automatically set as 
 // the last committed offset for the subscribed list of partitions
 // https://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
 //++
 long KConsumer::Consume(int64_t maxEvents, int64_t* numEvents, int64_t offset)
{
	long err = 0;

	if (m_topicPartition == NULL)
	{
		// Create the partition for this topic with offset (seek can only be called on actively fetched partitions)
		// https://github.com/confluentinc/confluent-kafka-go/issues/121
		err = CreatePartition(offset);
	}
	if (err != OK) return err;

	/*
	* Consume messages
	*/
	*numEvents = 0;
	//Clear the event queue
	m_consumerEvents.clear();
	//Clear the message count and bytes
	m_messageCount = 0;
	m_messageBytes = 0;


	rd_kafka_poll_set_consumer(m_consumer);
	m_errorLog.clear();
	while (s_run) {
		rd_kafka_message_t* msg = rd_kafka_consumer_poll(m_consumer, 100);
		if (!msg)
		{
			continue;	/* Timeout: no message within 100ms,
						*  try again. This short timeout allows
						*  checking for `run` at frequent intervals.
						*/
		}
		kafkaEvent event;
		err = ProcessMessage(msg, event);
		if (err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
		{
			//Done with partition - end of queue
			err = 0;
			rd_kafka_message_destroy(msg);
			break;
		}
		m_consumerEvents.push_back(event);
		if (maxEvents != -1)
		{
			if (m_messageCount >= maxEvents)
			{
				rd_kafka_message_destroy(msg);
				break;
			}
		}
		rd_kafka_message_destroy(msg);
	}
	if (err == OK)
	{
		*numEvents = m_consumerEvents.size();
		err = rd_kafka_commit(m_consumer, NULL, 0 /*sync*/);
	}
	
	return err;
}
//-------------------------------------------------------------------------------
//++ 
// Returns the pointer to the event queue
// Should be called only after ::Consume has returned successfully
//++
void KConsumer::GetData(std::vector<kafkaEvent>** events)
{
	*events = &m_consumerEvents;
}
//---------------------------------------------------------------------------------
//++
// Return the highest and lowest offsets for the topic_partition
//++
long KConsumer::GetMinMaxOffsets(int64_t* min, int64_t* max)
{	
	rd_kafka_resp_err_t err = rd_kafka_get_watermark_offsets(m_consumer, m_kafkaTopic.c_str(), m_partition, min, max);
	return err;
}
//--------------------------------------------------------------------------------------
// Sets the consumer offset to desired value
// Copied from github
// https://github.com/edenhill/librdkafka/blob/master/src-cpp/KafkaConsumerImpl.cpp
// Note - using hard coded timeout of 5s ... this is because I call thies function synchronously from consume()
//
long KConsumer::Seek(int64_t offset)
{
	rd_kafka_topic_t *rkt;

	if (!(rkt = rd_kafka_topic_new(m_consumer, m_kafkaTopic.c_str(), NULL)))
	{
		return rd_kafka_last_error();
	}
	long err;
	/* FIXME: Use a C API that takes a topic_partition_list_t instead */
	// I am not passing timeout of 0 ... so this is an synchronous call	
	err = rd_kafka_seek(rkt, m_partition, offset, 5000 /*timeout_ms*/);

	rd_kafka_topic_destroy(rkt);

	return err;
}
//-------------------------------------------------------------------------------------
// Private Methods
//-------------------------------------------------------------------------------------
long  KConsumer::ProcessMessage(rd_kafka_message_t* message, kafkaEvent& event)
{
	rd_kafka_resp_err_t errmsg = message->err;
	switch (errmsg)
	{
		case RD_KAFKA_RESP_ERR__TIMED_OUT:
			break;

		case RD_KAFKA_RESP_ERR_NO_ERROR:
		{
			/* Real message */
			m_messageCount++;
			m_messageBytes += message->len;
			event.offset = message->offset;

			rd_kafka_timestamp_type_t tsType;
			int64_t ts = rd_kafka_message_timestamp(message, &tsType);
			if (tsType != RD_KAFKA_TIMESTAMP_NOT_AVAILABLE) {
				event.tsname = "?";
				if (tsType == RD_KAFKA_TIMESTAMP_CREATE_TIME)
					event.tsname = "create time";
				else if (tsType == RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME)
					event.tsname = "log append time";
				event.timestamp = ts;
			}
			else
			{
				event.timestamp = 0;
			}
			if (message->key) {
				event.key.assign((const char*)message->key);
			}
			else
			{
				event.key = "";
			}
			event.msgLen = message->len;
			event.payload.assign(static_cast<const char*>(message->payload));
			break;
		}

		case RD_KAFKA_RESP_ERR__PARTITION_EOF:
			/* Last message */
			break;

		case RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:
		case RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION:
			break;

		default:
			/* Errors */
			m_errorString.assign("Consume failed");
	}
	return errmsg;
}

long KConsumer::CreatePartition(int64_t offset)
{
	rd_kafka_resp_err_t err;

	m_topicPartition = rd_kafka_topic_partition_list_new(1);
	if (!m_topicPartition)
	{
		m_errorString.assign("Failed to create topic partition: ");
		return OUT_OF_MEMORY;
	}
	rd_kafka_topic_partition_t* topicPartition = rd_kafka_topic_partition_list_add(m_topicPartition, m_kafkaTopic.c_str(), m_partition);
	err = rd_kafka_topic_partition_list_set_offset (m_topicPartition, m_kafkaTopic.c_str(), m_partition, offset);
	if (err)
	{
		return err;
	}
	/*
	* Assign specific partition
	*/
	err = rd_kafka_assign(m_consumer, m_topicPartition);

	return err;
}
void KConsumer::DestroyPartition(void)
{
	if (m_topicPartition)
	{
		rd_kafka_topic_partition_list_destroy(m_topicPartition);
		m_topicPartition = NULL;
	}
}
