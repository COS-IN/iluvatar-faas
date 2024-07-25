/*
 * Copyright (c) 2023 Georgios Alexopoulos
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Minimal CUDA declarations needed to compile gpushare.
 */

#ifndef _CUDA_DEFS_H
#define _CUDA_DEFS_H

/*
 * Give the preprocessor a chance to replace a function name.
 * The CUDA Driver API header file (cuda.h) uses #define directives which map
 * common names (e.g. cuMemAlloc) to more specific ones (e.g. cuMemAlloc_v2)
 * depending on library version.
 *
 * More on stringification: https://gcc.gnu.org/onlinedocs/gcc-3.4.6/cpp/Stringification.html
 */
#define STRINGIFY(x)                #x
#define CUDA_SYMBOL_STRING(x)       STRINGIFY(x)

#define cuMemGetInfo                cuMemGetInfo_v2
#define cuMemAlloc                  cuMemAlloc_v2
#define cuMemFree                   cuMemFree_v2
#define cuMemcpyHtoD                cuMemcpyHtoD_v2
#define cuMemcpyDtoH                cuMemcpyDtoH_v2
#define cuMemcpyDtoD                cuMemcpyDtoD_v2
#define cuMemcpyHtoDAsync           cuMemcpyHtoDAsync_v2
#define cuMemcpyDtoHAsync           cuMemcpyDtoHAsync_v2
#define cuMemcpyDtoDAsync           cuMemcpyDtoDAsync_v2

#define nvmlInit                    nvmlInit_v2
#define nvmlDeviceGetHandleByIndex  nvmlDeviceGetHandleByIndex_v2

#include <stdint.h>

typedef uint64_t cuuint64_t;
typedef unsigned long long CUdeviceptr_v2;
typedef CUdeviceptr_v2 CUdeviceptr;
typedef int CUdevice;

/* Opaque pointers */
typedef struct CUctx_st *CUcontext;
typedef struct CUstream_st *CUstream;
typedef struct CUfunc_st *CUfunction;
typedef struct nvmlDevice_st* nvmlDevice_t;

typedef enum cuda_drv_error_enum {
	CUDA_SUCCESS               = 0,
	CUDA_ERROR_OUT_OF_MEMORY   = 2,
	CUDA_ERROR_NOT_INITIALIZED = 3,
	CUDA_ERROR_UNKNOWN         = 999
} CUresult;

typedef enum CUmemAttach_flags_enum {
	CU_MEM_ATTACH_GLOBAL = 0x1
} CUmemAttach_flags;

#define CU_DEVICE_CPU ((CUdevice)-1)

typedef enum CUmem_advise_flags_enum {
	CU_MEM_ADVISE_SET_READ_MOSTLY = 0x1,
  CU_MEM_ADVISE_UNSET_READ_MOSTLY = 0x2,
  CU_MEM_ADVISE_SET_PREFERRED_LOCATION = 0x3,
  CU_MEM_ADVISE_UNSET_PREFERRED_LOCATION = 0x4,
  CU_MEM_ADVISE_SET_ACCESSED_BY = 0x5,
  CU_MEM_ADVISE_UNSET_ACCESSED_BY = 0x6
} CUmem_advise_flags;

#define CU_STREAM_PER_THREAD ((CUstream)0x2)

typedef enum CUdevice_attribute
{
  // Maximum number of threads per block
  CU_DEVICE_ATTRIBUTE_MAX_THREADS_PER_BLOCK = 1,
  // Maximum block dimension X
  CU_DEVICE_ATTRIBUTE_MAX_BLOCK_DIM_X = 2,
  // Maximum block dimension Y
  CU_DEVICE_ATTRIBUTE_MAX_BLOCK_DIM_Y = 3,
  // Maximum block dimension Z
  CU_DEVICE_ATTRIBUTE_MAX_BLOCK_DIM_Z = 4,
  // Maximum grid dimension X
  CU_DEVICE_ATTRIBUTE_MAX_GRID_DIM_X = 5,
  // Maximum grid dimension Y
  CU_DEVICE_ATTRIBUTE_MAX_GRID_DIM_Y = 6,
  // Maximum grid dimension Z
  CU_DEVICE_ATTRIBUTE_MAX_GRID_DIM_Z = 7,
  // Number of multiprocessors on device
  CU_DEVICE_ATTRIBUTE_MULTIPROCESSOR_COUNT = 16,
  // Maximum resident threads per multiprocessor
  CU_DEVICE_ATTRIBUTE_MAX_THREADS_PER_MULTIPROCESSOR = 39,
  // Maximum number of blocks per multiprocessor
  CU_DEVICE_ATTRIBUTE_MAX_BLOCKS_PER_MULTIPROCESSOR = 106,
} CUdevice_attribute_flags;

typedef struct Dims
{
  int x;
  int y;
  int z;
} Dims, *Dims_t;

typedef struct DeviceDimMax {
  int max_threads_per_block;
  int multiprocessor_count;
  int blocks_per_mp;
  int threads_per_mp;
  Dims block;
  Dims grid;
} DeviceDimMax, *DeviceDimMax_t;

typedef enum nvmlReturn_t_enum
{
  NVML_SUCCESS = 0,
  NVML_ERROR_UNKNOWN = 999
} nvmlReturn_t;

/*
 * Utilization information for a device.
 * Each sample period may be between 1 second and 1/6 second, depending on
 * the GPU being queried.
 */
typedef struct nvmlUtilization_st {
	/*
	 * Percent of time over the past sample period during which one or more
	 * kernels was executing on the GPU.
	 */
	unsigned int gpu;
	/*
	 * Percent of time over the past sample period during which global
	 * (device) memory was being read or written
	 */
	unsigned int memory;
} nvmlUtilization_t;

typedef enum CUstream_flags_enum {
  CU_STREAM_DEFAULT = 0x0,
  CU_STREAM_NON_BLOCKING = 0x1
} CUstream_flags;

/* typedefs for CUDA functions, to make hooking code cleaner */
typedef CUresult (*cuGetProcAddress_func)(const char *symbol, void **pfn, int cudaVersion, cuuint64_t flags);
typedef CUresult (*cuMemAllocManaged_func)(CUdeviceptr *dptr, size_t bytesize, unsigned int flags);
typedef CUresult (*cuMemAdvise_func)(CUdeviceptr devPtr, size_t count, CUmem_advise_flags advice, CUdevice device);
typedef CUresult (*cuMemPrefetchAsync_func)(CUdeviceptr devPtr, size_t count, CUdevice dstDevice, CUstream hStream);
typedef CUresult (*cuStreamCreate_func)(CUstream* phStream, unsigned int Flags); 
typedef CUresult (*cuDeviceGet_func)(CUdevice *device, int ordinal);
typedef CUresult (*cuMemAlloc_func)(CUdeviceptr *dptr, size_t bytesize);
typedef CUresult (*cuMemFree_func)(CUdeviceptr dptr);
typedef CUresult (*cuMemGetInfo_func)(size_t *free, size_t *total);
typedef CUresult (*cuGetErrorString_func)(CUresult error, const char **pStr);
typedef CUresult (*cuGetErrorName_func)(CUresult error, const char **pStr);
typedef CUresult (*cuCtxSetCurrent_func)(CUcontext ctx);
typedef CUresult (*cuCtxGetCurrent_func)(CUcontext *pctx);
typedef CUresult (*cuInit_func)(unsigned int flags);
typedef CUresult (*cuCtxSynchronize_func)(void);
typedef CUresult (*cuLaunchKernel_func)(CUfunction f, unsigned int gridDimX,
	unsigned int gridDimY, unsigned int gridDimZ, unsigned int blockDimX,
	unsigned int blockDimY, unsigned int blockDimZ,
	unsigned int sharedMemBytes, CUstream hStream, void **kernelParams,
	void **extra);
typedef CUresult (*cuMemcpy_func)(CUdeviceptr dst, CUdeviceptr src,
	size_t ByteCount);
typedef CUresult (*cuMemcpyAsync_func)(CUdeviceptr dst, CUdeviceptr src,
	size_t ByteCount, CUstream hStream);
typedef CUresult (*cuMemcpyDtoH_func)(void *dstHost, CUdeviceptr srcDevice,
	size_t ByteCount);
typedef CUresult (*cuMemcpyHtoD_func)(CUdeviceptr dstDevice,
	const void* srcHost, size_t ByteCount);
typedef CUresult (*cuMemcpyDtoD_func)(CUdeviceptr dstDevice,
	CUdeviceptr srcDevice, size_t ByteCount);
typedef CUresult (*cuMemcpyDtoHAsync_func)(void* dstHost,
	CUdeviceptr srcDevice, size_t ByteCount, CUstream hStream);
typedef CUresult (*cuMemcpyHtoDAsync_func)(CUdeviceptr dstDevice,
	const void* srcHost, size_t ByteCount, CUstream hStream);
typedef CUresult (*cuMemcpyDtoDAsync_func)(CUdeviceptr dstDevice,
	CUdeviceptr srcDevice, size_t ByteCount, CUstream hStream);
typedef CUresult (*cuStreamSynchronize_func)(CUstream hStream);
typedef CUresult (*cuDeviceGetAttribute_func)(int *pi, CUdevice_attribute_flags attrib, CUdevice dev);

typedef nvmlReturn_t (*nvmlDeviceGetUtilizationRates_func)(nvmlDevice_t device,
	nvmlUtilization_t *utilization);
typedef nvmlReturn_t (*nvmlInit_func)(void);
typedef nvmlReturn_t (*nvmlDeviceGetHandleByIndex_func)(unsigned int index,
	nvmlDevice_t *device);


/* Hooked CUDA functions */
extern CUresult cuGetProcAddress(const char *symbol, void **pfn,
	int cudaVersion, cuuint64_t flags);
extern CUresult cuMemGetInfo(size_t *free, size_t *total);
extern CUresult cuMemAlloc(CUdeviceptr *dptr, size_t bytesize);
extern CUresult cuMemAllocManaged(CUdeviceptr *dptr, size_t bytesize, enum CUmemAttach_flags_enum flags);
extern CUresult cuMemFree(CUdeviceptr dptr);
extern CUresult cuInit(unsigned int flags);
extern CUresult cuLaunchKernel(CUfunction f, unsigned int gridDimX,
	unsigned int gridDimY, unsigned int gridDimZ, unsigned int blockDimX,
	unsigned int blockDimY, unsigned int blockDimZ,
	unsigned int sharedMemBytes, CUstream hStream, void **kernelParams,
	void **extra);
// extern CUresult cuMemcpy(CUdeviceptr dst, CUdeviceptr src,
// 	size_t ByteCount);
// extern CUresult cuMemcpyAsync(CUdeviceptr dst, CUdeviceptr src,
// 	size_t ByteCount, CUstream hStream);
// extern CUresult cuMemcpyDtoH(void *dstHost, CUdeviceptr srcDevice,
// 	size_t ByteCount);
// extern CUresult cuMemcpyHtoD(CUdeviceptr dstDevice,
// 	const void* srcHost, size_t ByteCount);
// extern CUresult cuMemcpyDtoD(CUdeviceptr dstDevice,
// 	CUdeviceptr srcDevice, size_t ByteCount);
// extern CUresult cuMemcpyDtoHAsync(void* dstHost,
// 	CUdeviceptr srcDevice, size_t ByteCount, CUstream hStream);
// extern CUresult cuMemcpyHtoDAsync(CUdeviceptr dstDevice,
// 	const void* srcHost, size_t ByteCount, CUstream hStream);
// extern CUresult cuMemcpyDtoDAsync(CUdeviceptr dstDevice,
// 	CUdeviceptr srcDevice, size_t ByteCount, CUstream hStream);

/* Real CUDA functions */
extern cuGetProcAddress_func real_cuGetProcAddress;
extern cuMemAllocManaged_func real_cuMemAllocManaged;
extern cuMemFree_func real_cuMemFree;
extern cuMemGetInfo_func real_cuMemGetInfo;
extern cuGetErrorString_func real_cuGetErrorString;
extern cuGetErrorName_func real_cuGetErrorName;
extern cuCtxSetCurrent_func real_cuCtxSetCurrent;
extern cuCtxGetCurrent_func real_cuCtxGetCurrent;
extern cuInit_func real_cuInit;
extern cuCtxSynchronize_func real_cuCtxSynchronize;
extern cuLaunchKernel_func real_cuLaunchKernel;
extern cuMemcpy_func real_cuMemcpy;
extern cuMemcpyAsync_func real_cuMemcpyAsync;
extern cuMemcpyDtoH_func real_cuMemcpyDtoH;
extern cuMemcpyDtoHAsync_func real_cuMemcpyDtoHAsync;
extern cuMemcpyHtoD_func real_cuMemcpyHtoD;
extern cuMemcpyHtoDAsync_func real_cuMemcpyHtoDAsync;
extern cuMemcpyDtoD_func real_cuMemcpyDtoD;
extern cuMemcpyDtoDAsync_func real_cuMemcpyDtoDAsync;
extern cuStreamSynchronize_func real_cuStreamSynchronize;
extern cuDeviceGetAttribute_func real_cuDeviceGetAttribute;

extern void cuda_driver_check_error(CUresult err, const char *func_name);

/* NVML functions used to monitor the GPU utilization rate. */
extern nvmlDeviceGetUtilizationRates_func real_nvmlDeviceGetUtilizationRates;
extern nvmlInit_func real_nvmlInit;
extern nvmlDeviceGetHandleByIndex_func real_nvmlDeviceGetHandleByIndex;
extern CUcontext cuda_ctx;

extern int nvml_ok;

#endif /* _CUDA_DEFS_H */

