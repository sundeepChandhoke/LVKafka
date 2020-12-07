#pragma once

typedef struct _kafkaEvent
{
	std::string payload;
	std::string key;
	size_t msgLen;
	int64_t offset;
	int64_t timestamp;
	std::string tsname;
}kafkaEvent;

