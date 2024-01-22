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

This has been tested on Red Hat Enterprise Linux 8 and Ubuntu 22.04.

Before building, the following package must be installed on the system:
* UUID
  * On RHEL 8, use `sudo dnf install libuuid`
  * On Ubuntu 22.04, use `sudo apt install uuid-dev`

* RDKafka
  * Option 1 (recommended): Build from source
    * Download from `https://github.com/confluentinc/librdkafka.git`
    * `cd` to the download directory
    * `./configure`
    * `make`
    * `make install`
  * Option 2 (untested): Install packages
    * RHEL 8: `sudo dnf install librdkafka-devel`
    * Ubuntu 22.04: `sudo apt install librdkafka-dev`

For information on RDKafka, refer to (https://docs.confluent.io/kafka-clients/librdkafka/current/overview.html)

There is a Makefile in the `SemiAdv/KafkaLVLib/KafkaLV/KafkaLV` directory.
You may need to modify the LVDIR line in the Makefile, which is the location of the LabVIEW cintools directory.

You may wish to add the cintools and libLVKafkaLib.so to the OS search path.
1. Add a LVKafka.conf file to /etc/ld.so.conf.d/ which includes the cintools directory and the location of liblvkafka.so. 
2. Run "sudo ldconfig" to reload/recache the search paths.
