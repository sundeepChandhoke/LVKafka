// KafkaLV.cpp : Defines the exported functions for the DLL.
//


#include "SystemSpecifics.h"
#include <memory>
#include <map>
#include <vector>
#include <string>
#include "KafkaLV.h"
#include "Consumer.h"
#include "KafkaLVDataDef.h"
#include "Producer.h"
#include "extcode.h"

#define ASSERT(f) if(!(f)) *reinterpret_cast<int*>(0xbad) = __LINE__
//---------------------------------------------
typedef struct _tConsumerHandle
{
    std::unique_ptr<KConsumer> consumer = NULL;
    std::string brokerAddress;
    std::string topic;
    int partition;
}tConsumerHandle;
typedef struct _tProducerHandle
{
	std::unique_ptr<KProducer> producer = NULL;
	std::string brokerAddress;
	std::string topic;
}tProducerHandle;

kafkaWrapperErrors GetGuid(std::string& guidStr);

std::map<std::string, std::shared_ptr<tConsumerHandle>> g_consumerMap;
std::map<std::string, std::shared_ptr<tProducerHandle>> g_producerMap;


//--------------------------------------------------------------------
// Consumer Client Export
//--------------------------------------------------------------------
KAFKALV_API long KafkaCreateConsumer(char* kafkaBroker, char* topic, int32_t partition, char* consumerHandle)
{
    if (!kafkaBroker || !topic || !consumerHandle) return INVALID_PTR;
    if (strlen(consumerHandle) < GUIDSTRINGSIZE) return INVALID_SIZE;
    std::shared_ptr<tConsumerHandle> aConsumer = std::make_shared<tConsumerHandle>();
    std::string hnd;
    kafkaWrapperErrors herr = GetGuid(hnd);
    if (herr != OK)
    {
        return herr;
    }
	if (hnd.size() > strlen(consumerHandle))
	{
		return INVALID_SIZE;
	}
    aConsumer->brokerAddress = kafkaBroker;
    aConsumer->partition = partition;
    aConsumer->topic = topic;
    aConsumer->consumer = std::make_unique<KConsumer>();
    long err = aConsumer->consumer->Initialize(kafkaBroker, topic, partition);
    if (err != 0)
    {
        return UNDEFINED_ERROR;
    }
    g_consumerMap[hnd] = aConsumer;
	

	strncpy(consumerHandle, hnd.c_str(),strlen(consumerHandle));
    return OK;
}
//--------------------------------------------------------------------------------
// Close the Consumer Handle
//--------------------------------------------------------------------------------
KAFKALV_API long KafkaCloseConsumer(char* consumerHandle)
{
	if (!consumerHandle) return INVALID_PTR;
    long ret = OK;
    try
    {
        g_consumerMap.at(consumerHandle)->consumer.reset();
        g_consumerMap.erase(consumerHandle);
    }
    catch (std::out_of_range&)
    {
        ret = INVALID_ARG;
    }
    return ret;

}
//--------------------------------------------------------------------------------
// Blocking method that returns after end of partition is reached or 
// maxEvents have been collected
//--------------------------------------------------------------------------------
KAFKALV_API long ConsumeFromBeginning(char* consumerHandle, int64_t maxEvents, int64_t* numEvents)
{
    long ret = OK;
    try
    {
        ret = g_consumerMap.at(consumerHandle)->consumer->ConsumeFromBeginning(maxEvents, numEvents);
    }
    catch (std::out_of_range&)
    {
        ret = INVALID_ARG;
    }
    return ret;
}
KAFKALV_API long ConsumeFromEnd(char* consumerHandle, kEvent* events, int32_t count)
{
	long ret = OK;
	try
	{
		std::vector<kafkaEvent> consumerEvents;
		g_consumerMap.at(consumerHandle)->consumer->ConsumeFromEnd(consumerEvents, count);

		int64_t evNum = 0;
		for (kafkaEvent ev : consumerEvents)
		{
			events[evNum].payloadSize = ev.msgLen;
			if (ev.msgLen <= strlen(events[evNum].payload))
			{
				strncpy(events[evNum].payload, (char*)ev.payload.c_str(), strlen(events[evNum].payload));
			}
			else
			{
				return INVALID_SIZE;
			}
			events[evNum].keyLength = ev.key.size();
			if (ev.key.size() <= strlen(events[evNum].key))
			{
				strncpy(events[evNum].key, (char*)ev.key.c_str(), strlen(events[evNum].key));
			}
			else
			{
				return INVALID_SIZE;
			}

			events[evNum].offset = ev.offset;
			events[evNum].timestamp = ev.timestamp;
			events[evNum].tsnameLength = ev.tsname.size();
			if (ev.tsname.size() <= strlen(events[evNum].tsname))
			{
				strncpy(events[evNum].tsname, (char*)ev.tsname.c_str(), strlen(events[evNum].tsname));
			}
			else
			{
				return INVALID_SIZE;
			}
			evNum++;
		}
	}
	catch (std::out_of_range&)
	{
		ret = INVALID_ARG;
	}
	return ret;
}
//-------------------------------------------------------------------------------------
// Consume
//-------------------------------------------------------------------------------------
KAFKALV_API long Consume(char* consumerHandle, int64_t maxEvents, int64_t offset, int64_t* numEvents)
{
	long ret = OK;
	try
	{
		ret = g_consumerMap.at(consumerHandle)->consumer->Consume(maxEvents, numEvents, offset);
	}
	catch (std::out_of_range&)
	{
		ret = INVALID_ARG;
	}
	return ret;

}
//-----------------------------------------------------------------------------------
// Get min/max offsets
//-----------------------------------------------------------------------------------
KAFKALV_API long GetMinMaxOffsets(char* consumerHandle, int64_t* min, int64_t* max)
{
	long ret = OK;
	try
	{
		ret = g_consumerMap.at(consumerHandle)->consumer->GetMinMaxOffsets(min, max);
	}
	catch (std::out_of_range&)
	{
		ret = INVALID_ARG;
	}
	return ret;
}
//--------------------------------------------------------------------------------
// Method that returns the data collected
//--------------------------------------------------------------------------------
KAFKALV_API long GetData(char* consumerHandle, int64_t count, kEvent* events, int64_t* fetched)
{
    long ret = OK;
    try
    {
        std::vector<kafkaEvent>* consumerEvents;
        g_consumerMap.at(consumerHandle)->consumer->GetData(&consumerEvents);

        if (consumerEvents->size() > (size_t)count)
        {
            *fetched = 0;
            return INVALID_SIZE;
        }

        int64_t evNum = 0;
        for (kafkaEvent ev : *consumerEvents)
        {
            events[evNum].payloadSize = ev.msgLen;
			if (ev.msgLen <= strlen(events[evNum].payload))
			{
				strncpy(events[evNum].payload, (char*)ev.payload.c_str(), strlen(events[evNum].payload));
			}
			else
			{
				return INVALID_SIZE;
			}
            events[evNum].keyLength = ev.key.size();
			if (ev.key.size() <= strlen(events[evNum].key))
			{
				strncpy(events[evNum].key, (char*)ev.key.c_str(), strlen(events[evNum].key));
			}
			else
			{
				return INVALID_SIZE;
			}
            events[evNum].offset = ev.offset;
            events[evNum].timestamp = ev.timestamp;

            events[evNum].tsnameLength = ev.tsname.size();
			if (ev.tsname.size() <= strlen(events[evNum].tsname))
			{
				strncpy(events[evNum].tsname, (char*)ev.tsname.c_str(), strlen(events[evNum].tsname));
			}
			else
			{
				return INVALID_SIZE;
			}
            evNum++;
			*fetched = evNum;
        }
    }
    catch (std::out_of_range&)
    {
        ret = INVALID_ARG;
    }
    return ret;
}
//Seek can only be called on actively fetched partitions
KAFKALV_API long ConsumerSeek(char* consumerHandle, int64_t offset)
{
	long ret = OK;
	try
	{
		ret = g_consumerMap.at(consumerHandle)->consumer->Seek(offset);
	}
	catch (std::out_of_range&)
	{
		ret = INVALID_ARG;
	}
	return ret;
}
//Seek can only be called on actively fetched partitions
KAFKALV_API long ConsumerExitLoop(char* consumerHandle)
{
	kafkaWrapperErrors ret = OK;
	try
	{
		g_consumerMap.at(consumerHandle)->consumer->ExitLoop();
	}
	catch (std::out_of_range&)
	{
		ret = INVALID_ARG;
	}
	return ret;
}
//--------------------------------------------------------------------------------
// Private Methods
//--------------------------------------------------------------------------------
kafkaWrapperErrors GetGuid(std::string& guidStr)
{
	kafkaWrapperErrors err = OK;
#ifdef _WIN32
    UUID guid;
    long ret = CoCreateGuid(&guid);
    if (ret == S_OK)
    {
		kafkaWrapperErrors err = OK;
        //https://stackoverflow.com/questions/607651/how-many-characters-are-there-in-a-guid
        char guid_cstr[GUIDSTRINGSIZE];
        snprintf(guid_cstr, sizeof(guid_cstr),
            "{%08x-%04x-%04x-%02x%02x-%02x%02x%02x%02x%02x%02x}",
            guid.Data1, guid.Data2, guid.Data3,
            guid.Data4[0], guid.Data4[1], guid.Data4[2], guid.Data4[3],
            guid.Data4[4], guid.Data4[5], guid.Data4[6], guid.Data4[7]);

        guidStr.assign(guid_cstr);
    }
	else
	{
		err = UNDEFINED_ERROR;
	}
    return err;
#elif __linux__

	uuid_t guid;
	//Generate a unique ID
	uuid_generate(guid);
	char guidChar[36];
	//Convert the unique id type to char
	uuid_unparse(guid, guidChar);
	guidStr.assign(guidChar);
	return OK;
#endif
}
//-----------------------------------------------------------------------------------------
// Producer Client Export
//-----------------------------------------------------------------------------------------
KAFKALV_API long KafkaCreateProducer(char* kafkaBroker, char* topic, double bufferingTimeMS, char* producerHandle)
{
	if (!kafkaBroker || !topic || !producerHandle) return INVALID_PTR;
	if (strlen(producerHandle) < GUIDSTRINGSIZE) return INVALID_SIZE;
	std::shared_ptr<tProducerHandle> aProducer = std::make_shared<tProducerHandle>();
	std::string hnd;
	kafkaWrapperErrors herr = GetGuid(hnd);
	if (herr != OK)
	{
		return herr;
	}
	if (hnd.size() > strlen(producerHandle))
	{
		return INVALID_SIZE;
	}

	aProducer->brokerAddress = kafkaBroker;
	aProducer->topic = topic;
	aProducer->producer = std::make_unique<KProducer>();
	long err = aProducer->producer->Initialize(kafkaBroker, topic, bufferingTimeMS);
	if (herr != 0)
	{
		return UNDEFINED_ERROR;
	}
	else
	g_producerMap[hnd] = aProducer;
	

	strncpy(producerHandle, hnd.c_str(),strlen(producerHandle));

	return OK;
}
//--------------------------------------------------------------------------------
// Close the Producer Handle
//--------------------------------------------------------------------------------
KAFKALV_API long KafkaCloseProducer(char* producerHandle)
{
	if (!producerHandle) return INVALID_PTR;
	kafkaWrapperErrors ret = OK;
	try
	{
		g_producerMap.at(producerHandle)->producer.reset();
		g_producerMap.erase(producerHandle);
	}
	catch (std::out_of_range&)
	{
		ret = INVALID_ARG;
	}
	return ret;

}

//-----------------------------------------------------------------------------------------
// LabVIEW support
//-----------------------------------------------------------------------------------------
class tLVConnector
{

public:
    tLVConnector()
    {
    }

    void resize1DArray(void*** hndPtr, size_t size, int32_t eltAlignment = 1)
    {
        int32_t retVal = 0;
	if (eltAlignment > 4)
	    retVal = NumericArrayResize(0xa, 1, reinterpret_cast<UHandle *>(hndPtr), (static_cast<int32_t>(size) + 7) / 8);
	else
	    retVal = NumericArrayResize(0x1, 1, reinterpret_cast<UHandle *>(hndPtr), static_cast<int32_t>(size));
        ASSERT(retVal == 0);
    }
};
tLVConnector gLVConnector;
class tLVString
{
    struct tArray {
        int32_t m_len;
        char    m_data[4];
    };

    tArray** m_hnd;
    tLVString() = delete;
public:
    tLVString& operator=(const std::string& rhs)
    {
      gLVConnector.resize1DArray(reinterpret_cast<void***>(&m_hnd), rhs.size());
        (*m_hnd)->m_len = static_cast<int32_t>(rhs.size());
        memcpy(data(), rhs.data(), rhs.size());
        return *this;
    }
    size_t len() { return m_hnd == nullptr ? 0 : static_cast<size_t>((*m_hnd)->m_len); }
    char* data() { return m_hnd == nullptr ? nullptr : (*m_hnd)->m_data; }
    void copy(std::string& dst)
    {
        if (len() == 0) {
            dst = "";
        }
        else {
            dst.assign(data(), len());
        }
    }
};
//class tKafkaEvent
//{
//    friend class tLVKafkaEvent;
//
//protected:
//    std::string m_payload;
//    std::string m_key;
//    size_t      m_msgLen;
//    int64_t     m_offset;
//    int64_t     m_timestamp;
//    std::string m_tsname;
//
//public:
//    tKafkaEvent() {}
//    tKafkaEvent& operator =(tLVKafkaEvent& rhs);
//};
class tLVKafkaEvent
{

protected:
    tLVString   m_payload;
    tLVString   m_key;
    uint64_t    m_msgLen;
    int64_t     m_offset;
    int64_t     m_timestamp;
    tLVString   m_tsname;

    tLVKafkaEvent() = delete;

public:
    tLVKafkaEvent& operator =(kafkaEvent& rhs)
    {
        m_payload = rhs.payload;
        m_key = rhs.key;
        m_msgLen = static_cast<uint64_t>(rhs.msgLen);
        m_offset = rhs.offset;
        m_timestamp = rhs.timestamp;
        m_tsname = rhs.tsname;

        return *this;
    }
	void copyProducerEvent(kafkaEvent& event)
	{
		m_payload.copy(event.payload);
		m_key.copy(event.key);
		event.msgLen = static_cast<size_t>(m_msgLen);
		event.offset = m_offset;
		event.timestamp = m_timestamp;
		m_tsname.copy(event.tsname);
	}
};

class tLVAligned1DArray
{
    struct tArray {
        int32_t m_len;
        int32_t m_padding;
        char    m_data[4];
    };
    tArray** m_hnd;
    tLVAligned1DArray() = delete;
public:
    size_t len() { return m_hnd == nullptr ? 0 : static_cast<size_t>((*m_hnd)->m_len); }

    template <typename T>
    T* data() { return m_hnd == nullptr ? nullptr : reinterpret_cast<T*>((*m_hnd)->m_data); }

    template <typename T>
    void resize(size_t numElements, int32_t eltAlignment = 1)
    {
        gLVConnector.resize1DArray(reinterpret_cast<void***>(&m_hnd), numElements * sizeof(T), eltAlignment);
        (*m_hnd)->m_len = static_cast<int32_t>(numElements);
    }
};
//--------------------------------------------------------------------------------
// LabVIEW Exports
//--------------------------------------------------------------------------------
KAFKALV_API int32_t LVGetData(char* consumerHandle, tLVAligned1DArray& hnd)
{
	try
	{
		std::vector<kafkaEvent>* events;

		// Add code to get the events
		g_consumerMap.at(consumerHandle)->consumer->GetData(&events);

		if (events->size() == 0) {
			if (hnd.len() == 0) {
				// Nothing to do here
				return OK;
			}
			hnd.resize<tLVKafkaEvent>(0);
			return OK;
		}

		hnd.resize<tLVKafkaEvent>(events->size(), 8);
		auto lvData = hnd.data<tLVKafkaEvent>();
		for (size_t i = 0; i < events->size(); i++) {
			lvData[i] = (*events)[i];
		}

		return OK;
	}
	catch (std::out_of_range&)
	{
		return INVALID_ARG;
	}
	return OK;
}
KAFKALV_API int32_t LVConsumeFromEnd(char* consumerHandle, tLVAligned1DArray& hnd, int32_t count)
{
	try {
		long herr = OK;
		std::vector<kafkaEvent> events;

		// Add code to get the events
		herr = g_consumerMap.at(consumerHandle)->consumer->ConsumeFromEnd(events, count);
		if (events.size() == 0) {
			if (hnd.len() == 0) {
				// Nothing to do here
				return OK;
			}
			hnd.resize<tLVKafkaEvent>(0);
			return OK;
		}

		hnd.resize<tLVKafkaEvent>(events.size(), 8);
		auto lvData = hnd.data<tLVKafkaEvent>();
		for (size_t i = 0; i < events.size(); i++) {
			lvData[i] = events[i];
		}

		return OK;
	}
	catch (std::out_of_range&)
	{
		return INVALID_ARG;
	}
	return OK;
}
KAFKALV_API int32_t LVSendData(char* producerHandle, int32_t partition, tLVAligned1DArray hnd)
{
	try{
		size_t len = hnd.len();
		std::vector<kafkaEvent> events;
		events.resize(len);

		auto lvData = hnd.data<tLVKafkaEvent>();
		for (size_t i = 0; i < len; i++) {
			//events[i] = lvData[i];
			lvData[i].copyProducerEvent(events[i]);
		}
	

		return g_producerMap.at(producerHandle)->producer->SendEvents(partition, events);
	}
	catch (std::out_of_range&)
	{
		return INVALID_ARG;
	}
	return OK;
}

