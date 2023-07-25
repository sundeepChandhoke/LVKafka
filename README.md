# LVKafka
LabVIEW Kafka DLL and VI APIs. 
The code contains the LV wrapper to librdkafka library from Confluent.
The VIs and examples show how to call this DLL.

# Windows build

When building for Windows the following depedencies have been included in the repository. 

In the ..\Windows\includes\ directory, there is C++ source information that is specific to the Windows build. This directory should be added to the includes for the build. These files are included in the SystemSpecifics.h file.

In the ..\librdkafka\ directory, the .h, .lib, and .dll files for rdkafka are included.

The extCode.h file is provided with LabVIEW. The path to the file should be in your includes, which is specific to the LabVIEW version that will be used (e.g. C:\Program Files\National Instruments\LabVIEW 2019\cintools).

Add the path to the DLL to the LabVIEW VI Search paths to allow Call Library Function Nodes to find the DLL when referenced as LVKafkaLib.*.

# Linux build

The provided shared object file was tested on Red Hat Enterprise Linux 8. 

In the Linux directory, the makefile and respective depedencies (sources.mk and subdir.mk) have been provided for building the shared library.

Before building with the makefile, the following packages must be installed on the system:
1. libuuid-devel 
2. librdkafka-devel - for information on refer to (https://docs.confluent.io/kafka-clients/librdkafka/current/overview.html)

In subdir.mk, modify the -I cintools path for the version of LabVIEW that is installed on the system (e.g. -I/usr/local/natinst/LabVIEW-2020-64/cintools).

The directory for extCode.h and libLVKafkaLib.so need to be added to the OS search path.
1. Add a .conf file to /etc/ld.so.conf.d/ which includes the cintools directory and the location of liblvkafka.so. 
2. Run "sudo ldconfig" to reconfigure the search paths.
3. Reboot the system to allow the changes to take effect.