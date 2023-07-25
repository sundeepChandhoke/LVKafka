
#include "stdint.h"
#include "SystemSpecifics.h"

#ifdef __cplusplus
extern "C" {
#endif

//Consumer

KAFKALV_API long KafkaCreateConsumer(char* kafkaBroker, char* topic, int32_t partition, char* consumerHandle);
KAFKALV_API long ConsumeFromBeginning(char* consumerHandle, int64_t maxEvents, int64_t* numEvents);
KAFKALV_API long Consume(char* consumerHandle, int64_t maxEvents, int64_t offset, int64_t* numEvents);
KAFKALV_API long GetMinMaxOffsets(char* consumerHandle, int64_t* min, int64_t* max);
KAFKALV_API long ConsumerSeek(char* consumerHandle, int64_t offset);
KAFKALV_API long ConsumerExitLoop(char* consumerHandle);
KAFKALV_API long KafkaCloseConsumer(char* consumerHandle);

typedef struct _tkEvent
{
	char* payload;
	int64_t payloadSize;
	char* key;
	int64_t keyLength;
	int64_t offset;
	int64_t timestamp;
	char* tsname;
	int64_t tsnameLength;
}kEvent;
//
// [in] consumerHandle - obtained when KafkaCreateConsumer was created
KAFKALV_API long GetData(char* consumerHandle, int64_t count, kEvent* events, int64_t* fetched);
KAFKALV_API long ConsumeFromEnd(char* consumerHandle, kEvent *events, int32_t count);

//LVExports
class tLVAligned1DArray;
KAFKALV_API int32_t LVGetData(char* consumerHandle, tLVAligned1DArray& hnd);
KAFKALV_API int32_t LVConsumeFromEnd(char* consumerHandle, tLVAligned1DArray& hnd, int32_t count);

//Producer

KAFKALV_API long KafkaCreateProducer(char* kafkaBroker, char* topic, double bufferingTime, char* producerHandle);
KAFKALV_API long KafkaCloseProducer(char* producerHandle);

//LVExport
KAFKALV_API int32_t LVSendData(char* producerHandle, int32_t partition, tLVAligned1DArray hnd);
#ifdef __cplusplus
}
#endif
