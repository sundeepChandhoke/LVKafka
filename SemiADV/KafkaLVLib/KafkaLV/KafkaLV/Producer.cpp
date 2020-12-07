
#include "pch.h"
#include "Producer.h"
/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2019, Magnus Edenhill
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
  * Apache Kafka producer
  * using the Kafka driver from librdkafka
  * (https://github.com/edenhill/librdkafka)
  */

//static volatile sig_atomic_t run = 1;
std::vector<std::string> KProducer::m_errorLog;

//static void sigterm(int sig) {
//	run = 0;
//}
//Construction/Destruction
KProducer::KProducer()
{
	m_kafkaTopic.assign("qamData");
	m_lingerMS.assign("100");
	m_producer = NULL;
}

KProducer::~KProducer()
{
	if (m_producer)
	{
		rd_kafka_destroy(m_producer);
		m_producer = NULL;
	}
}
//Public Methods
long KProducer::Initialize(std::string kafkaBroker, std::string topic, double linger_ms)
{
	m_kafkaTopic.assign(topic);
	m_lingerMS = std::to_string(linger_ms);
	return Initialize(kafkaBroker);
}
long KProducer::Initialize(std::string kafkaBroker)
{
	m_kafkaBroker.assign(kafkaBroker);

	/*
	 * Create configuration object
	 */
	
	rd_kafka_conf_t* conf = rd_kafka_conf_new();
	if (!conf)
	{
		return E_OUTOFMEMORY;
	}
	char errstr[512];

	/* Set bootstrap broker(s) as a comma-separated list of
	 * host or host:port (default port 9092).
	 * librdkafka will use the bootstrap brokers to acquire the full
	 * set of brokers from the cluster. */
	if (rd_kafka_conf_set(conf, "bootstrap.servers", m_kafkaBroker.c_str(), errstr, sizeof(errstr)) !=
		RD_KAFKA_CONF_OK) {
		m_errorString.assign(errstr);
		rd_kafka_conf_destroy(conf);
		return E_FAIL;
	}
	//https://docs.confluent.io/5.5.0/clients/librdkafka/md_CONFIGURATION.html
	/* Set linger.ms 
	 * Delay in milliseconds to wait for messages in the producer queue to accumulate 
	 * before constructing message batches (MessageSets) to transmit to brokers */
	if (rd_kafka_conf_set(conf, "linger.ms", m_lingerMS.c_str(), errstr, sizeof(errstr)) !=
		RD_KAFKA_CONF_OK) {
		m_errorString.assign(errstr);
		rd_kafka_conf_destroy(conf);
		return E_FAIL;
	}
	/* Set the delivery report callback.
		 * This callback will be called once per message to inform
		 * the application if delivery succeeded or failed.
		 * See dr_msg_cb() above.
		 * The callback is only triggered from rd_kafka_poll() and
		 * rd_kafka_flush(). */
	rd_kafka_conf_set_dr_msg_cb(conf, KProducer::dr_msg_cb);
	/*
		 * Create producer instance.
		 *
		 * NOTE: rd_kafka_new() takes ownership of the conf object
		 *       and the application must not reference it again after
		 *       this call.
		 */
	m_producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!m_producer) {
		m_errorString.assign(errstr);
		return E_NOINTERFACE;
	}

	return S_OK;
}

long KProducer::SendEvents(int32_t partition, std::vector<kafkaEvent>& events)
{
	m_errorLog.clear();
	for (kafkaEvent event : events) 
	{
		if (event.payload.empty()) {
			rd_kafka_poll(m_producer, 0/*non-blocking */);
			continue;
		}

		/*
		 * Send/Produce message.
		 * This is an asynchronous call, on success it will only
		 * enqueue the message on the internal producer queue.
		 * The actual delivery attempts to the broker are handled
		 * by background threads.
		 * The previously registered delivery report callback
		 * is used to signal back to the application when the message
		 * has been delivered (or failed permanently after retries).
		 */
	retry:
		rd_kafka_resp_err_t err = rd_kafka_producev(
														/* Producer handle */
														m_producer,
														/* Topic name */
														RD_KAFKA_V_TOPIC(m_kafkaTopic.c_str()),
														/* Partition*/
														RD_KAFKA_V_PARTITION(partition),
														/*Key*/
														RD_KAFKA_V_KEY(event.key.c_str(), event.key.size()),
														/* Make a copy of the payload. */
														RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
														/* Message value and length */
														RD_KAFKA_V_VALUE(event.payload.c_str(), event.payload.size()),
														/*timestamp*/
														RD_KAFKA_V_TIMESTAMP(event.timestamp),
														/* Per-Message opaque, provided in
														 * delivery report callback as
														 * msg_opaque. */
														RD_KAFKA_V_OPAQUE(NULL),
														/* End sentinel */
														RD_KAFKA_V_END
													);
			

		if (err != RD_KAFKA_RESP_ERR_NO_ERROR) 
		{
			m_errorLog.push_back("Failed to produce to topic");

			if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL)
			{
				/* If the internal queue is full, wait for
				* messages to be delivered and then retry.
				* The internal queue represents both
				* messages to be sent and messages that have
				* been sent or failed, awaiting their
				* delivery report callback to be called.
				*
				* The internal queue is limited by the
				* configuration property
				* queue.buffering.max.messages */
				rd_kafka_poll(m_producer, 1000/*block for max 1000ms*/);
				goto retry;
			}
		}
		else 
		{
			m_errorLog.push_back("Enqueued message " +
								std::to_string(event.payload.size()) +
								std::string(" bytes") +
								std::string(" for topic ") + m_kafkaTopic 
			);
		}

		/* A producer application should continually serve
		* the delivery report queue by calling rd_kafka_poll()
		* at frequent intervals.
		* Either put the poll call in your main loop, or in a
		* dedicated thread, or call it after every
		* rd_kafka_produce() call.
		* Just make sure that rd_kafka_poll() is still called
		* during periods where you are not producing any messages
		* to make sure previously produced messages have their
		* delivery report callback served (and any other callbacks
		* you register). */
		rd_kafka_poll(m_producer, 0/*non-blocking*/);
	}

	/* Wait for final messages to be delivered or fail.
	* rd_kafka_flush() is an abstraction over rd_kafka_poll() which
	* waits for all messages to be delivered. */
	m_errorLog.push_back("Flushing final messages");
	rd_kafka_flush(m_producer, 10 * 1000 /* wait for max 10 seconds */);

	/* If the output queue is still not empty there is an issue
		* with producing messages to the clusters. */
	if (int len = rd_kafka_outq_len(m_producer) > 0)
	{
		m_errorLog.push_back(std::to_string(len) +
			std::string(" message(s) were not delivered")
		);
	}
	return S_OK;
}

/**
 * @brief Message delivery report callback.
 *
 * This callback is called exactly once per message, indicating if
 * the message was succesfully delivered
 * (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) or permanently
 * failed delivery (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR).
 *
 * The callback is triggered from rd_kafka_poll() and executes on
 * the application's thread.
 */
void KProducer::dr_msg_cb(rd_kafka_t* rk,
	const rd_kafka_message_t* rkmessage, void* opaque) {
	if (rkmessage->err)
	{
		m_errorLog.push_back(
			std::string("Message delivery failed : ") +
			std::to_string(rkmessage->len) 

		);
	}
	else
	{
		m_errorLog.push_back(
			std::string("Message delivered : ") +
			std::to_string(rkmessage->err) +
			std::string(" to partition : ") +
			std::to_string(rkmessage->partition)
		);
	}
	/* The rkmessage is destroyed automatically by librdkafka */
}