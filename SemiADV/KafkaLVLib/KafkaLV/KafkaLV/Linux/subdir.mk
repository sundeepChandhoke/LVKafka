################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../Consumer.cpp \
../KafkaLV.cpp \
../Producer.cpp 

CPP_DEPS += \
./Consumer.d \
./KafkaLV.d \
./Producer.d 

OBJS += \
./Consumer.o \
./KafkaLV.o \
./Producer.o 


# Each subdirectory must supply rules for building sources it contributes
%.o: ../%.cpp subdir.mk
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -O3 -Wall -c -fmessage-length=0 -fpermissive -I/usr/local/natinst/LabVIEW-2020-64/cintools -I/usr/include/librdkafka -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$@" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


clean: clean--2e-

clean--2e-:
	-$(RM) ./Consumer.d ./Consumer.o ./KafkaLV.d ./KafkaLV.o ./Producer.d ./Producer.o

.PHONY: clean--2e-

