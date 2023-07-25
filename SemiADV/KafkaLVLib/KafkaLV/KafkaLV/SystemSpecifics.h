#pragma once
// Systems specific definitions

// The following ifdef block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the KAFKALV_EXPORTS
// symbol defined on the command line. This symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// KAFKALV_API functions as being imported from a DLL, whereas this DLL sees symbols
// defined with this macro as being exported.

#ifdef _WIN32

#ifdef KAFKALV_EXPORTS
#define KAFKALV_API extern "C" __declspec(dllexport)
#else
#define KAFKALV_API __declspec(dllimport)
#endif

// Necessary headers for Windows
#include "pch.h"
#include "framework.h"
#include "combaseapi.h"
#include "extcode.h"

// Windows uses a size 38 GUID string
#define GUIDSTRINGSIZE 38

#elif __linux__

#include "uuid/uuid.h"

#define KAFKALV_API __attribute__((visibility("default")))

//Linux UUID string size
#define GUIDSTRINGSIZE 36

#endif

// Define error enum that is OS agnostic
enum kafkaWrapperErrors
{
	OK = 0, // no error
	INVALID_SIZE = 1, // the expected data size did not match the actual size
	INVALID_ARG = 2, // one or more arquments are invalid
	INVALID_PTR = 3, // the pointer is invalid
	FAIL = 4,		 // an internal failure has occurred
	NO_INTERFACE = 5, // the interface is not supported
	OUT_OF_MEMORY = 6, // ran out of memory
	UNDEFINED_ERROR = 99 // undefined error
};