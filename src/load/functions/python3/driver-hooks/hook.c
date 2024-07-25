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
 *
 * Hook CUDA function calls.
 */

/*
 * Defining _GNU_SOURCE allows us to call dlvsym().
 *
 * More on _GNU_SOURCE: https://stackoverflow.com/a/5583764
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* _GNU_SOURCE */

#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <inttypes.h>
#include <stdbool.h>

#include "common.h"
#include "cuda_defs.h"
#include "utlist.h"

#define ENV_GPUSHARE_ENABLE_SINGLE_OVERSUB  "GPUSHARE_ENABLE_SINGLE_OVERSUB"

#define MEMINFO_RESERVE_MIB 1536           /* MiB */
#define KERN_SYNC_DURATION_BIG 10          /* seconds */
#define KERN_SYNC_WINDOW_STEPDOWN_THRESH 1 /* seconds */
#define KERN_SYNC_WINDOW_MAX 2048          /* Pending Kernels */

static void *real_dlsym_225(void *handle, const char *symbol);

cuCtxSynchronize_func real_cuCtxSynchronize = NULL;
cuLaunchKernel_func real_cuLaunchKernel = NULL;
cuMemcpy_func real_cuMemcpy = NULL;
cuMemcpyAsync_func real_cuMemcpyAsync = NULL;
cuMemcpyDtoH_func real_cuMemcpyDtoH = NULL;
cuMemcpyDtoHAsync_func real_cuMemcpyDtoHAsync = NULL;
cuMemcpyHtoD_func real_cuMemcpyHtoD = NULL;
cuMemcpyHtoDAsync_func real_cuMemcpyHtoDAsync = NULL;
cuMemcpyDtoD_func real_cuMemcpyDtoD = NULL;
cuMemcpyDtoDAsync_func real_cuMemcpyDtoDAsync = NULL;
cuGetProcAddress_func real_cuGetProcAddress = NULL;
cuMemAllocManaged_func real_cuMemAllocManaged = NULL;
cuMemAdvise_func real_cuMemAdvise = NULL;
cuMemPrefetchAsync_func real_cuMemPrefetchAsync = NULL;
cuDeviceGet_func real_cuDeviceGet = NULL;
cuMemAlloc_func real_cuMemAlloc = NULL;
cuStreamCreate_func real_cuStreamCreate = NULL;
cuMemFree_func real_cuMemFree = NULL;
cuMemGetInfo_func real_cuMemGetInfo = NULL;
cuGetErrorString_func real_cuGetErrorString = NULL;
cuGetErrorName_func real_cuGetErrorName = NULL;
cuCtxSetCurrent_func real_cuCtxSetCurrent = NULL;
cuCtxGetCurrent_func real_cuCtxGetCurrent = NULL;
cuStreamSynchronize_func real_cuStreamSynchronize = NULL;
cuInit_func real_cuInit = NULL;
cuDeviceGetAttribute_func real_cuDeviceGetAttribute = NULL;

nvmlDeviceGetUtilizationRates_func real_nvmlDeviceGetUtilizationRates = NULL;
nvmlInit_func real_nvmlInit = NULL;
nvmlDeviceGetHandleByIndex_func real_nvmlDeviceGetHandleByIndex = NULL;

size_t gpushare_size_mem_allocatable = 0;
size_t sum_allocated = 0;

int kern_since_sync = 0;
int pending_kernel_window = 1;
pthread_mutex_t kcount_mutex;

int enable_single_oversub = 0;
int nvml_ok = 1;
CUdevice assignedDevice;
CUstream asyncPrefetchStream = NULL;
bool async_prefetch_outstanding = false;
bool on_device = true;
DeviceDimMax dims;

CUresult
get_compute_limits(DeviceDimMax *dims)
{
  log_debug("getting compute limits");

  int attribute = 0;
  CUDA_CHECK_ERR(real_cuDeviceGetAttribute(&attribute, CU_DEVICE_ATTRIBUTE_MAX_THREADS_PER_BLOCK, assignedDevice), CUDA_SYMBOL_STRING(cuDeviceGetAttribute));
  dims->max_threads_per_block = max(attribute, 1);

  CUDA_CHECK_ERR(real_cuDeviceGetAttribute(&attribute, CU_DEVICE_ATTRIBUTE_MULTIPROCESSOR_COUNT, assignedDevice), CUDA_SYMBOL_STRING(cuDeviceGetAttribute));
  dims->multiprocessor_count = max(attribute, 1);

  CUDA_CHECK_ERR(real_cuDeviceGetAttribute(&attribute, CU_DEVICE_ATTRIBUTE_MAX_BLOCKS_PER_MULTIPROCESSOR, assignedDevice), CUDA_SYMBOL_STRING(cuDeviceGetAttribute));
  dims->blocks_per_mp = max(attribute, 1);

  CUDA_CHECK_ERR(real_cuDeviceGetAttribute(&attribute, CU_DEVICE_ATTRIBUTE_MAX_THREADS_PER_MULTIPROCESSOR, assignedDevice), CUDA_SYMBOL_STRING(cuDeviceGetAttribute));
  dims->threads_per_mp = max(attribute, 1);

  CUDA_CHECK_ERR(real_cuDeviceGetAttribute(&attribute, CU_DEVICE_ATTRIBUTE_MAX_BLOCK_DIM_X, assignedDevice), CUDA_SYMBOL_STRING(cuDeviceGetAttribute));
  dims->block.x = max(attribute, 1);
  CUDA_CHECK_ERR(real_cuDeviceGetAttribute(&attribute, CU_DEVICE_ATTRIBUTE_MAX_BLOCK_DIM_Y, assignedDevice), CUDA_SYMBOL_STRING(cuDeviceGetAttribute));
  dims->block.y = max(attribute, 1);
  CUDA_CHECK_ERR(real_cuDeviceGetAttribute(&attribute, CU_DEVICE_ATTRIBUTE_MAX_BLOCK_DIM_Z, assignedDevice), CUDA_SYMBOL_STRING(cuDeviceGetAttribute));
  dims->block.z = max(attribute, 1);

  CUDA_CHECK_ERR(real_cuDeviceGetAttribute(&attribute, CU_DEVICE_ATTRIBUTE_MAX_GRID_DIM_X, assignedDevice), CUDA_SYMBOL_STRING(cuDeviceGetAttribute));
  dims->grid.x = max(attribute, 1);
  CUDA_CHECK_ERR(real_cuDeviceGetAttribute(&attribute, CU_DEVICE_ATTRIBUTE_MAX_GRID_DIM_Y, assignedDevice), CUDA_SYMBOL_STRING(cuDeviceGetAttribute));
  dims->grid.y = max(attribute, 1);
  CUDA_CHECK_ERR(real_cuDeviceGetAttribute(&attribute, CU_DEVICE_ATTRIBUTE_MAX_GRID_DIM_Z, assignedDevice), CUDA_SYMBOL_STRING(cuDeviceGetAttribute));
  dims->grid.z = max(attribute, 1);
  log_debug("device properties:");
  log_debug("max_threads_per_block=%d; SMP count: %d; Blocks per SMP: %d; Threads per SMP: %d", 
            dims->max_threads_per_block, dims->multiprocessor_count, dims->blocks_per_mp, dims->threads_per_mp);
  log_debug("block = %d * %d * %d; dim = %d * %d * %d", dims->block.x, dims->block.y, dims->block.z, dims->grid.x, dims->grid.y, dims->grid.z);
  return CUDA_SUCCESS;
}

int prefetchToDevice();
void ensure_on_device() {
  if (!on_device) {
    log_debug("Memory not on device, moving");
    prefetchToDevice();
  } else {
    log_debug("Memory on device");
  }
}

void check_async_prefetch() {
  if (async_prefetch_outstanding) {
    if (asyncPrefetchStream == NULL) {
      log_debug("Synchronizing default stream");
      real_cuStreamSynchronize(CU_STREAM_PER_THREAD);
    } else {
      log_debug("Synchronizing custom stream");
      real_cuStreamSynchronize(asyncPrefetchStream);
    }
  }
  async_prefetch_outstanding = false;
}

/* Representation of a CUDA memory allocation */
struct cuda_mem_allocation {
	CUdeviceptr ptr;
	size_t size;
	struct cuda_mem_allocation *next;
};

/* Linked list that holds all memory allocations of current application. */
struct cuda_mem_allocation *cuda_allocation_list = NULL;

/* Load real CUDA {Driver API, NVML} functions and bootstrap auxiliary stuff. */
static void bootstrap_cuda(void)
{
	char *error;
	void *cuda_handle;
	void *nvml_handle;


	true_or_exit(pthread_mutex_init(&kcount_mutex, NULL) == 0);

	nvml_handle = dlopen("libnvidia-ml.so.1", RTLD_LAZY);
	if (!nvml_handle) {
		error = dlerror();
		log_debug("%s", error);
		nvml_ok = 0;
	} else {
		dlerror();
		real_nvmlDeviceGetUtilizationRates =
			(nvmlDeviceGetUtilizationRates_func)real_dlsym_225(nvml_handle,
			CUDA_SYMBOL_STRING(nvmlDeviceGetUtilizationRates));
		error = dlerror();
		if (error != NULL) {
			log_debug("%s", error);
			nvml_ok = 0;
		}
		real_nvmlInit = (nvmlInit_func)
		real_dlsym_225(nvml_handle,CUDA_SYMBOL_STRING(nvmlInit));
		error = dlerror();
		if (error != NULL) {
			log_debug("%s", error);
			nvml_ok = 0;
		}
		real_nvmlDeviceGetHandleByIndex = (nvmlDeviceGetHandleByIndex_func)
		real_dlsym_225(nvml_handle,
			CUDA_SYMBOL_STRING(nvmlDeviceGetHandleByIndex));
		error = dlerror();
		if (error != NULL) {
			log_debug("%s", error);
			nvml_ok = 0;
		}
	}
	if (nvml_ok) log_debug("Found NVML");
	else log_debug("Could not find NVML");

	cuda_handle = dlopen("libcuda.so", RTLD_LAZY);
	if (!cuda_handle) {
		error = dlerror();
		log_fatal("%s", error);
	}
	/*
	 * For dlsym(), a return value of NULL does not necessarily indicate
	 * an error. Therefore, we must:
	 *  1. clear the previous error state by calling dlerror()
	 *  2. call dlsym()
	 *  3. call dlerror()
	 * If the value which dlerror() returns is not NULL, an error occured.
	 */
	dlerror();
	real_cuMemAllocManaged = (cuMemAllocManaged_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuMemAllocManaged));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
  real_cuMemAdvise = (cuMemAdvise_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuMemAdvise));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
  real_cuMemPrefetchAsync = (cuMemPrefetchAsync_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuMemPrefetchAsync));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
  real_cuDeviceGet = (cuDeviceGet_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuDeviceGet));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuMemAlloc = (cuMemAlloc_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuMemAlloc));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuStreamCreate = (cuStreamCreate_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuStreamCreate));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);

	real_cuMemFree = (cuMemFree_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuMemFree));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuGetProcAddress = (cuGetProcAddress_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuGetProcAddress));
	error = dlerror();
	if (error != NULL)
		/*
		 * Print a debug message instead of failing immediately, since
		 * this symbol may not be used. This may be the case for CUDA
		 * Runtime <11.3.
		 */
		log_debug("%s", error);
	real_cuMemGetInfo = (cuMemGetInfo_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuMemGetInfo));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuGetErrorString = (cuGetErrorString_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuGetErrorString));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuGetErrorName = (cuGetErrorString_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuGetErrorName));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuCtxSetCurrent = (cuCtxSetCurrent_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuCtxSetCurrent));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuCtxGetCurrent = (cuCtxGetCurrent_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuCtxGetCurrent));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuInit = (cuInit_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuInit));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuCtxSynchronize = (cuCtxSynchronize_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuCtxSynchronize));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuLaunchKernel = (cuLaunchKernel_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuLaunchKernel));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuMemcpy = (cuMemcpy_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuMemcpy));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuMemcpyAsync = (cuMemcpyAsync_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuMemcpyAsync));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuMemcpyDtoH = (cuMemcpyDtoH_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuMemcpyDtoH));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuMemcpyDtoHAsync = (cuMemcpyDtoHAsync_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuMemcpyDtoHAsync));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuMemcpyHtoD = (cuMemcpyHtoD_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuMemcpyHtoD));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuMemcpyHtoDAsync = (cuMemcpyHtoDAsync_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuMemcpyHtoDAsync));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuMemcpyDtoD = (cuMemcpyDtoD_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuMemcpyDtoD));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuMemcpyDtoDAsync = (cuMemcpyDtoDAsync_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuMemcpyDtoDAsync));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
	real_cuStreamSynchronize = (cuStreamSynchronize_func)
		real_dlsym_225(cuda_handle,CUDA_SYMBOL_STRING(cuStreamSynchronize));
	error = dlerror();
	if (error != NULL)
		log_fatal("%s", error);
  real_cuDeviceGetAttribute = (cuDeviceGetAttribute_func)
      real_dlsym_225(cuda_handle, CUDA_SYMBOL_STRING(cuDeviceGetAttribute));
  error = dlerror();
  if (error != NULL)
    log_fatal("%s", error);
}

/* Return the allocation in MB */
double total_cuda_allocations()
{
  return toMiB(sum_allocated);
}

/* Append a new CUDA memory allocation at the end of the list. */
static void insert_cuda_allocation(CUdeviceptr dptr, size_t bytesize)
{
	struct cuda_mem_allocation *allocation;


	sum_allocated += bytesize;
	log_debug("Total allocated memory on GPU is %.2f MiB",
		  toMiB(sum_allocated));

	true_or_exit(allocation = malloc(sizeof(*allocation)));

	allocation->ptr = dptr;
	allocation->size = bytesize;
	allocation->next = NULL;
	LL_APPEND(cuda_allocation_list, allocation);
}

/* Remove a CUDA memory allocation given the pointer it starts at */
static void remove_cuda_allocation(CUdeviceptr rm_ptr)
{
	struct cuda_mem_allocation *tmp, *a;


	LL_FOREACH_SAFE(cuda_allocation_list, a, tmp) {
		if (a->ptr == rm_ptr) {
			sum_allocated -= a->size;
			log_debug("Total allocated memory on GPU is %.2f MiB",
				  toMiB(sum_allocated));
			LL_DELETE(cuda_allocation_list, a);
			free(a);
		}
	}
}


/* Toggle debug mode and single process oversubscription based on envvars */
static void initialize_libgpushare(void)
{
	char *value;
	value = getenv(ENV_GPUSHARE_DEBUG);
	if (value != NULL)
		__debug = 1;	
	value = getenv(ENV_GPUSHARE_ENABLE_SINGLE_OVERSUB);
	if (value != NULL) {
		enable_single_oversub = 1;
		log_warn("Enabling GPU memory oversubscription for this"
		         " application");
	}

	bootstrap_cuda();
}


/*
 * Check the return value of a CUDA Driver API function call for errors.
 *
 * Interpret using the Driver API functions:
 * - cuGetErrorString
 * - cuGetErrorName
 */
void cuda_driver_check_error(CUresult err, const char *func_name)
{
	if (err != CUDA_SUCCESS) {
		const char *err_string;
		const char *err_name;
		real_cuGetErrorString(err, &err_string);
		real_cuGetErrorName(err, &err_name);
		log_warn("%s returned %s: %s",
	                 func_name, err_name, err_string);
	}
}


/* 
 * Since we're interposing dlsym() in libgpushare, we use dlvsym() to obtain the
 * address of the real dlsym function.
 *
 * Depending on glibc version, we look for the appropriate symbol.
 *
 * Some context on the implementation:
 *
 * glibc 2.34 remove the internal __libc_dlsym() symbol that NVIDIA uses in
 * their cuHook example:
 * https://github.com/phrb/intro-cuda/blob/d38323b81cd799dc09179e2ef27aa8f81b6dac40/src/cuda-samples/7_CUDALibraries/cuHook/libcuhook.cpp#L43
 *
 * One solution, discussed in apitrace's repo is to use dlvsym(), which also
 * takes a version string as a 3rd argument, in order to obtain the real
 * dlsym().
 * 
 * This is what user 'manisandro' suggested 8 years ago, when warning about
 * using the private __libc_dlsym():
 * https://github.com/apitrace/apitrace/issues/258
 * 
 * The maintainer of the repo didn't heed the warning back then, it came back
 * 8 years later and bit them.
 * 
 * This is also what user "derhass" suggests:
 * https://stackoverflow.com/a/18825060
 * (See section "UPDATE FOR 2021/glibc-2.34").
 * 
 * Given all the above, we obtain the real `dlsym()` as such:
 * real_dlsym=dlvsym(RTLD_NEXT, "dlsym", "GLIBC_2.2.5");
 *
 * Since we have to explicitly use a version argument in dlvsym(), we also have
 * to define and export two versions of dlsym (hence the linker script.), one
 * for each distinct glibc symbol version.
 *
 */
static void *real_dlsym_225(void *handle, const char *symbol)
{

	typedef void *(dlsym_t)(void *, const char *);
	static dlsym_t *r_dlsym;
	char *err;


	if (!r_dlsym) {
		dlerror();
		r_dlsym = (dlsym_t*)dlvsym(RTLD_NEXT, "dlsym", "GLIBC_2.2.5");
		err = dlerror();
		if (err != NULL)
			log_fatal("%s", err);
	}

	return (*r_dlsym)(handle, symbol);
}

static void *real_dlsym_231(void *handle, const char *symbol)
{
	typedef void *(dlsym_t)(void *, const char *);
	static dlsym_t *r_dlsym;
	char *err;


	if (!r_dlsym) {
		dlerror();
		r_dlsym = (dlsym_t*)dlvsym(RTLD_NEXT, "dlsym", "GLIBC_2.31");
		err = dlerror();
		if (err != NULL)
			log_fatal("%s", err);
	}

	return (*r_dlsym)(handle, symbol);
}

static void *real_dlsym_234(void *handle, const char *symbol)
{
	typedef void *(dlsym_t)(void *, const char *);
	static dlsym_t *r_dlsym;
	char *err;


	if (!r_dlsym) {
		dlerror();
		r_dlsym = (dlsym_t*)dlvsym(RTLD_NEXT, "dlsym", "GLIBC_2.34");
		err = dlerror();
		if (err != NULL)
			log_fatal("%s", err);
	}

	return (*r_dlsym)(handle, symbol);
}

void dump_symbol(void *handle, const char *symbol){
    UNUSED(handle);
	if (strncmp(symbol, "cu", 2) == 0) {
        log_debug("[dlsym]%s", symbol);
    }
}

/*
 * CUDA Runtime API uses dlopen()/dlsym() to obtain addresses of the Driver API
 * functions.
 *
 * [spoiler: from CUDA 11.3 onwards, it only uses dlsym() to get the address
 *  of cuGetProcAddress() and then uses the latter to obtain the addresses
 *  of all other Driver API functions/symbols.]
 *
 * When the user program calls dlsym() requesting a Driver API symbol, return
 * our interposed version.
 *
 * In all other cases, call the real dlsym() from glibc and pass on the
 * requested symbol string.
 */
void *dlsym_225(void *handle, const char *symbol)
{
    // log_debug("%s:%d called", __FUNCTION__, __LINE__);
    // dump_symbol( handle, symbol );
  // return (real_dlsym_225(handle, symbol));

	if (strncmp(symbol, "cu", 2) != 0) {
		return (real_dlsym_225(handle, symbol));
	} else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemAlloc)) == 0) {
		return (void *)(&cuMemAlloc);
	} else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemAllocManaged)) == 0) {
		return (void *)(&cuMemAllocManaged);
	} else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemFree)) == 0) {
		return (void *)(&cuMemFree);
	} else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemGetInfo)) == 0) {
		return (void *)(&cuMemGetInfo);
	} else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuGetProcAddress)) == 0) {
		return (void *)(&cuGetProcAddress);
	} else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuInit)) == 0) {
		return (void *)(&cuInit);
//   }
	} else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuLaunchKernel)) == 0) {
		return (void *)(&cuLaunchKernel);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpy)) == 0) {
	// 	return (void *)(&cuMemcpy);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyAsync)) == 0) {
	// 	return (void *)(&cuMemcpyAsync);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyDtoH)) == 0) {
	// 	return (void *)(&cuMemcpyDtoH);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyDtoHAsync)) == 0) {
	// 	return (void *)(&cuMemcpyDtoHAsync);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyHtoD)) == 0) {
	// 	return (void *)(&cuMemcpyHtoD);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyHtoDAsync)) == 0) {
	// 	return (void *)(&cuMemcpyHtoDAsync);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyDtoD)) == 0) {
	// 	return (void *)(&cuMemcpyDtoD);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyDtoDAsync)) == 0) {
	// 	return (void *)(&cuMemcpyDtoDAsync);
	}

	return (real_dlsym_225(handle, symbol));
}

void *dlsym_234(void *handle, const char *symbol)
{
  // log_debug("%s:%d called", __FUNCTION__, __LINE__);
  // return (real_dlsym_234(handle, symbol));
	if (strncmp(symbol, "cu", 2) != 0) {
		return (real_dlsym_234(handle, symbol));
	} else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemAlloc)) == 0) {
		return (void *)(&cuMemAlloc);
	} else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemFree)) == 0) {
		return (void *)(&cuMemFree);
	} else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemGetInfo)) == 0) {
		return (void *)(&cuMemGetInfo);
	} else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuGetProcAddress)) == 0) {
		return (void *)(&cuGetProcAddress);
	} else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuInit)) == 0) {
		return (void *)(&cuInit);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuLaunchKernel)) == 0) {
	// 	return (void *)(&cuLaunchKernel);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpy)) == 0) {
	// 	return (void *)(&cuMemcpy);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyAsync)) == 0) {
	// 	return (void *)(&cuMemcpyAsync);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyDtoH)) == 0) {
	// 	return (void *)(&cuMemcpyDtoH);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyDtoHAsync)) == 0) {
	// 	return (void *)(&cuMemcpyDtoHAsync);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyHtoD)) == 0) {
	// 	return (void *)(&cuMemcpyHtoD);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyHtoDAsync)) == 0) {
	// 	return (void *)(&cuMemcpyHtoDAsync);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyDtoD)) == 0) {
	// 	return (void *)(&cuMemcpyDtoD);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyDtoDAsync)) == 0) {
	// 	return (void *)(&cuMemcpyDtoDAsync);
	}

	return (real_dlsym_234(handle, symbol));
}

void *dlsym_231(void *handle, const char *symbol)
{
  // log_debug("%s:%d called", __FUNCTION__, __LINE__);
  // return (real_dlsym_231(handle, symbol));
	if (strncmp(symbol, "cu", 2) != 0) {
		return (real_dlsym_231(handle, symbol));
	} else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemAlloc)) == 0) {
		return (void *)(&cuMemAlloc);
	} else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemFree)) == 0) {
		return (void *)(&cuMemFree);
	} else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemGetInfo)) == 0) {
		return (void *)(&cuMemGetInfo);
	} else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuGetProcAddress)) == 0) {
		return (void *)(&cuGetProcAddress);
	} else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuInit)) == 0) {
		return (void *)(&cuInit);
//   }
	} else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuLaunchKernel)) == 0) {
		return (void *)(&cuLaunchKernel);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpy)) == 0) {
	// 	return (void *)(&cuMemcpy);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyAsync)) == 0) {
	// 	return (void *)(&cuMemcpyAsync);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyDtoH)) == 0) {
	// 	return (void *)(&cuMemcpyDtoH);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyDtoHAsync)) == 0) {
	// 	return (void *)(&cuMemcpyDtoHAsync);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyHtoD)) == 0) {
	// 	return (void *)(&cuMemcpyHtoD);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyHtoDAsync)) == 0) {
	// 	return (void *)(&cuMemcpyHtoDAsync);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyDtoD)) == 0) {
	// 	return (void *)(&cuMemcpyDtoD);
	// } else if (strcmp(symbol, CUDA_SYMBOL_STRING(cuMemcpyDtoDAsync)) == 0) {
	// 	return (void *)(&cuMemcpyDtoDAsync);
	}

	return (real_dlsym_231(handle, symbol));
}


/*
 * Older CUDA Runtime API (version <=11.2) does the following during internal
 * initialization (when the user program calls it for the first time):
 * 1. Calls dlopen("libcuda.so.1")
 * 2. Calls dlsym() for each function in the Driver API
 * 
 * Newer CUDA Runtime API (version >=11.3) works like this:
 * 1. Calls dlopen("libcuda.so.1") and then dlsym("cuGetProcAddress")
 * 2. Calls cuGetProcAddress("cuGetProcAddress")
 *     1. If the pointer to "cuGetProcAddress" is NULL, it falls back to using
 *        dlsym() to get the Driver API function pointers
 *     2. If the pointer to "cuGetProcAddress" is not NULL, it uses
 *        cuGetProcAddress to get the Driver API function pointers.
 *
 * Interpose both, to cover all cases.
 * 
 * The logic is the same as when interposing dlsym().
 */
CUresult cuGetProcAddress(const char *symbol, void **pfn, int cudaVersion,
	cuuint64_t flags)
{
	CUresult result = CUDA_SUCCESS;

	if (real_cuGetProcAddress == NULL) return CUDA_ERROR_NOT_INITIALIZED;

	if (strcmp(symbol, "cuMemAlloc") == 0) {
		*pfn = (void *)(&cuMemAlloc);
	} else if (strcmp(symbol, "cuMemAllocManaged") == 0) {
		*pfn = (void *)(&cuMemFree);
	} else if (strcmp(symbol, "cuMemFree") == 0) {
		*pfn = (void *)(&cuMemFree);
	} else if (strcmp(symbol, "cuMemGetInfo") == 0) {
		*pfn = (void *)(&cuMemGetInfo);
	} else if (strcmp(symbol, "cuGetProcAddress") == 0) {
		*pfn = (void *)(&cuGetProcAddress);
	} else if (strcmp(symbol, "cuInit") == 0) {
		*pfn = (void *)(&cuInit);
	} else if (strcmp(symbol, "cuLaunchKernel") == 0) {
		*pfn = (void *)(&cuLaunchKernel);
	} else {
		result = real_cuGetProcAddress(symbol, pfn, cudaVersion, flags);
	}
  // else if (strcmp(symbol, "cuMemcpy") == 0) {
	// 	*pfn = (void *)(&cuMemcpy);
	// } else if (strcmp(symbol, "cuMemcpyAsync") == 0) {
	// 	*pfn = (void *)(&cuMemcpyAsync);
	// } else if (strcmp(symbol, "cuMemcpyDtoH") == 0) {
	// 	*pfn = (void *)(&cuMemcpyDtoH);
	// } else if (strcmp(symbol, "cuMemcpyDtoHAsync") == 0) {
	// 	*pfn = (void *)(&cuMemcpyDtoHAsync);
	// } else if (strcmp(symbol, "cuMemcpyHtoD") == 0) {
	// 	*pfn = (void *)(&cuMemcpyHtoD);
	// } else if (strcmp(symbol, "cuMemcpyHtoDAsync") == 0) {
	// 	*pfn = (void *)(&cuMemcpyHtoDAsync);
	// } else if (strcmp(symbol, "cuMemcpyDtoD") == 0) {
	// 	*pfn = (void *)(&cuMemcpyDtoD);
	// } else if (strcmp(symbol, "cuMemcpyDtoDAsync") == 0) {
	// 	*pfn = (void *)(&cuMemcpyDtoDAsync);
	// } else {
	// 	result = real_cuGetProcAddress(symbol, pfn, cudaVersion, flags);
	// }

	return result;
}

/*
  Use a custom stream to prefetch all allocated memory to the host
*/
int prefetchStreamToHost() {
  CUresult result = CUDA_SUCCESS;
	struct cuda_mem_allocation *tmp, *a;
  if (real_cuMemAdvise == NULL) return CUDA_ERROR_NOT_INITIALIZED;
  if (real_cuStreamCreate == NULL) return CUDA_ERROR_NOT_INITIALIZED;
  if (asyncPrefetchStream == NULL)
    CUDA_CHECK_ERR(real_cuStreamCreate(&asyncPrefetchStream, CU_STREAM_NON_BLOCKING), CUDA_SYMBOL_STRING(cuStreamCreate));

	LL_FOREACH_SAFE(cuda_allocation_list, a, tmp) {
      // result = real_cuMemAdvise(a->ptr, a->size, CU_MEM_ADVISE_SET_PREFERRED_LOCATION, CU_DEVICE_CPU);
      // CUDA_CHECK_ERR(result, CUDA_SYMBOL_STRING(cuMemAdvise));
      result = real_cuMemPrefetchAsync(a->ptr, a->size, CU_DEVICE_CPU, asyncPrefetchStream);
      CUDA_CHECK_ERR(result, CUDA_SYMBOL_STRING(cuMemPrefetchAsync));
      async_prefetch_outstanding = true;
	}
  return result;
}
/*
  Use a custom stream to prefetch all allocated memory to the device
*/
int prefetchStreamToDevice() {
  CUresult result = CUDA_SUCCESS;
	struct cuda_mem_allocation *tmp, *a;
  if (real_cuStreamCreate == NULL) return CUDA_ERROR_NOT_INITIALIZED;
  if (asyncPrefetchStream == NULL)
    CUDA_CHECK_ERR(real_cuStreamCreate(&asyncPrefetchStream, CU_STREAM_NON_BLOCKING), CUDA_SYMBOL_STRING(cuStreamCreate));

	LL_FOREACH_SAFE(cuda_allocation_list, a, tmp) {
    // result = real_cuMemAdvise(a->ptr, a->size, CU_MEM_ADVISE_SET_PREFERRED_LOCATION, assignedDevice);
    // CUDA_CHECK_ERR(result, CUDA_SYMBOL_STRING(cuMemAdvise));
    result = real_cuMemPrefetchAsync(a->ptr, a->size, assignedDevice, asyncPrefetchStream);
    CUDA_CHECK_ERR(result, CUDA_SYMBOL_STRING(cuMemPrefetchAsync));
    async_prefetch_outstanding = true;
	}
  return result;
}

/*
  Use the default stream to prefetch all allocated memory to the host
*/
int prefetchToHost() {
  CUresult result = CUDA_SUCCESS;
	struct cuda_mem_allocation *tmp, *a;
  if (real_cuMemAdvise == NULL) return CUDA_ERROR_NOT_INITIALIZED;

	LL_FOREACH_SAFE(cuda_allocation_list, a, tmp) {
      // result = real_cuMemAdvise(a->ptr, a->size, CU_MEM_ADVISE_SET_PREFERRED_LOCATION, CU_DEVICE_CPU);
      // CUDA_CHECK_ERR(result, CUDA_SYMBOL_STRING(cuMemAdvise));
      result = real_cuMemPrefetchAsync(a->ptr, a->size, CU_DEVICE_CPU, CU_STREAM_PER_THREAD);
      CUDA_CHECK_ERR(result, CUDA_SYMBOL_STRING(cuMemPrefetchAsync));
      async_prefetch_outstanding = true;
      on_device = false;
	}
  return result;
}
/*
  Use the default stream to prefetch all allocated memory to the device
*/
int prefetchToDevice() {
  CUresult result = CUDA_SUCCESS;
	struct cuda_mem_allocation *tmp, *a;

	LL_FOREACH_SAFE(cuda_allocation_list, a, tmp) {
    // result = real_cuMemAdvise(a->ptr, a->size, CU_MEM_ADVISE_SET_PREFERRED_LOCATION, assignedDevice);
    // CUDA_CHECK_ERR(result, CUDA_SYMBOL_STRING(cuMemAdvise));
    result = real_cuMemPrefetchAsync(a->ptr, a->size, assignedDevice, CU_STREAM_PER_THREAD);
    CUDA_CHECK_ERR(result, CUDA_SYMBOL_STRING(cuMemPrefetchAsync));
    async_prefetch_outstanding = true;
    on_device = true;
	}
  return result;
}

int madviseToHost() {
  CUresult result = CUDA_SUCCESS;
	struct cuda_mem_allocation *tmp, *a;
  if (real_cuMemAdvise == NULL) return CUDA_ERROR_NOT_INITIALIZED;

	LL_FOREACH_SAFE(cuda_allocation_list, a, tmp) {
      result = real_cuMemAdvise(a->ptr, a->size, CU_MEM_ADVISE_SET_PREFERRED_LOCATION, CU_DEVICE_CPU);
      CUDA_CHECK_ERR(result, CUDA_SYMBOL_STRING(cuMemAdvise));
      on_device = false;
	}
  return result;
}
int madviseToDevice() {
  CUresult result = CUDA_SUCCESS;
	struct cuda_mem_allocation *tmp, *a;
  if (real_cuDeviceGet == NULL) return CUDA_ERROR_NOT_INITIALIZED;
  if (real_cuMemAdvise == NULL) return CUDA_ERROR_NOT_INITIALIZED;

	LL_FOREACH_SAFE(cuda_allocation_list, a, tmp) {
      result = real_cuMemAdvise(a->ptr, a->size, CU_MEM_ADVISE_SET_PREFERRED_LOCATION, assignedDevice);
      CUDA_CHECK_ERR(result, CUDA_SYMBOL_STRING(cuMemAdvise));
      on_device = true;
	}
  return result;
}

CUresult cuMemAllocManaged(CUdeviceptr *dptr, size_t bytesize, enum CUmemAttach_flags_enum flags)
{
	CUresult result = CUDA_SUCCESS;

	/* Return immediately if not initialized */
	if (real_cuMemAllocManaged == NULL) return CUDA_ERROR_NOT_INITIALIZED;

	// log_debug("cuMemAllocManaged requested %zu bytes", bytesize);
	result = real_cuMemAllocManaged(dptr, bytesize, flags);
	cuda_driver_check_error(result, CUDA_SYMBOL_STRING(cuMemAllocManaged));
	// log_debug("cuMemAllocManaged allocated %zu bytes at 0x%llx", bytesize, *dptr);
	// result = real_cuMemAdvise(*dptr, bytesize, CU_MEM_ADVISE_SET_PREFERRED_LOCATION, assignedDevice);
	// cuda_driver_check_error(result, CUDA_SYMBOL_STRING(cuMemAdvise));
  // result = real_cuMemAdvise(*dptr, bytesize, CU_MEM_ADVISE_SET_ACCESSED_BY, assignedDevice);
	// cuda_driver_check_error(result, CUDA_SYMBOL_STRING(cuMemAdvise));
	// log_debug("cuMemAllocManaged cuMemAdvise set advice");
  
	if (result == CUDA_SUCCESS) {
		insert_cuda_allocation(*dptr, bytesize);
	}

	return result;
}

CUresult cuMemAlloc(CUdeviceptr *dptr, size_t bytesize)
{
	CUresult result = CUDA_SUCCESS;

	/* Return immediately if not initialized */
	if (real_cuMemAllocManaged == NULL) return CUDA_ERROR_NOT_INITIALIZED;

	// log_debug("cuMemAlloc requested %zu bytes", bytesize);
	result = real_cuMemAllocManaged(dptr, bytesize, CU_MEM_ATTACH_GLOBAL);
	cuda_driver_check_error(result, CUDA_SYMBOL_STRING(cuMemAllocManaged));
	// log_debug("cuMemAlloc allocated %zu bytes at 0x%llx", bytesize, *dptr);
	// result = real_cuMemAdvise(*dptr, bytesize, CU_MEM_ADVISE_SET_PREFERRED_LOCATION, assignedDevice);
	// cuda_driver_check_error(result, CUDA_SYMBOL_STRING(cuMemAdvise));
  // result = real_cuMemAdvise(*dptr, bytesize, CU_MEM_ADVISE_SET_ACCESSED_BY, assignedDevice);
	// cuda_driver_check_error(result, CUDA_SYMBOL_STRING(cuMemAdvise));
	// log_debug("cuMemAlloc cuMemAdvise set advice");
  
	if (result == CUDA_SUCCESS) {
		insert_cuda_allocation(*dptr, bytesize);
	}

	return result;
}


CUresult cuMemFree(CUdeviceptr dptr)
{
	CUresult result = CUDA_SUCCESS;

	if (real_cuMemFree == NULL) return CUDA_ERROR_NOT_INITIALIZED;
	result = real_cuMemFree(dptr);
	if (result == CUDA_SUCCESS) remove_cuda_allocation(dptr);

	return result;
}


CUresult cuMemGetInfo(size_t *free, size_t *total)
{
	long long reserve_mib;
	CUresult result = CUDA_SUCCESS;


	/* Return immediately if not initialized */
	if (real_cuMemGetInfo == NULL) return CUDA_ERROR_NOT_INITIALIZED;

	result = real_cuMemGetInfo(free, total);
	cuda_driver_check_error(result, CUDA_SYMBOL_STRING(cuMemGetInfo));

	log_debug("real_cuMemGetInfo returned free=%.2f MiB, total=%.2f MiB",
		 toMiB(*free), toMiB(*total));

        /*
	 * Hide a static amount of GPU memory from the applications. CUDA uses
	 * this memory to store context information and it is not pageable.
	 *
	 * In practice, this amount of memory is not static and depends on
	 * the number of colocated applications. Each one has its own context,
	 * which eats away some physical, non-pageable GPU memory.
	 *
	 * The first application that runs theoretically has (TOTAL_GPU_MEM - 
	 * CONTEXT_SIZE) memory available.
	 *
	 * CONTEXT_SIZE typically uses a few hundred MB and depends on the GPU
	 * model.
	 *
	 * cuBLAS and other CUDA libraries also eat away at this memory.
	 *
	 * When another app runs, this "working memory size" shrinks further
	 * and can lead to thrashing within the first application, even when
	 * it runs alone.
	 *
	 * We cannot shrink the memory allocations of a running app, and the
	 * app thinks all of its memory is physically backed, since it's
	 * programmed with cuMemAlloc semantics in mind.
	 *
	 * To avoid internal thrashing, we empirically choose a sane value for
	 * MEMINFO_RESERVE_MIB.
	 */
	reserve_mib = (MEMINFO_RESERVE_MIB) MiB;
	*free = *total - (size_t) reserve_mib;

	log_debug("gpushare's cuMemGetInfo returning free=%.2f MiB,"
		  " total=%.2f MiB", toMiB(*free), toMiB(*total));
	return result;
}

/*
 * A call to cuInit is an indicator that the present application is a CUDA
 * application and that we should bootstrap gpushare.
 */
CUresult cuInit(unsigned int flags)
{
	CUresult result = CUDA_SUCCESS;
	static pthread_once_t init_libgpushare_done = PTHREAD_ONCE_INIT;

	true_or_exit(pthread_once(&init_libgpushare_done, initialize_libgpushare) == 0);

	result = real_cuInit(flags);
	cuda_driver_check_error(result, CUDA_SYMBOL_STRING(cuInit));

  if (real_cuDeviceGet == NULL) {
    log_warn("real_cuDeviceGet was not set after bootstrap");
    return CUDA_ERROR_UNKNOWN;
  }
  if (real_cuMemAdvise == NULL) {
    log_warn("real_cuMemAdvise was not set after bootstrap");
    return CUDA_ERROR_UNKNOWN;
  }
  CUDA_CHECK_ERR(real_cuDeviceGet(&assignedDevice, 0), CUDA_SYMBOL_STRING(cuDeviceGet));

  result = get_compute_limits(&dims);
  
  return result;
}

int outstanding_kernels = 0;

CUresult cuLaunchKernel(CUfunction f, unsigned int gridDimX,
	unsigned int gridDimY, unsigned int gridDimZ, unsigned int blockDimX,
	unsigned int blockDimY, unsigned int blockDimZ,
	unsigned int sharedMemBytes, CUstream hStream, void **kernelParams,
	void **extra)
{
	CUresult result = CUDA_SUCCESS;
  // unsigned int maxDim = 64;
  // unsigned int maxBlock = 16;

  /* Return immediately if not initialized */
  if (real_cuLaunchKernel == NULL)
    return CUDA_ERROR_NOT_INITIALIZED;

  // printf("out: %d Old Invoking kernel with dim block: %u * %u * %u ; grid: %u * %u * %u \n", outstanding_kernels,
  //      blockDimX, blockDimY, blockDimZ, gridDimX, gridDimY, gridDimZ);
  // gridDimX = min(gridDimX, maxDim);
  // gridDimY = min(gridDimY, maxDim);
  // gridDimZ = min(gridDimZ, maxDim);
  // blockDimX = min(blockDimX, maxBlock);
  // blockDimY = min(blockDimY, maxBlock);
  // blockDimZ = min(blockDimZ, maxBlock);
  ++outstanding_kernels;
  printf("New Invoking kernel with dim block: %u * %u * %u ; grid: %u * %u * %u \n",
         blockDimX, blockDimY, blockDimZ, gridDimX, gridDimY, gridDimZ);
  result = real_cuLaunchKernel(f, gridDimX, gridDimY, gridDimZ, blockDimX,
                               blockDimY, blockDimZ, sharedMemBytes, hStream, kernelParams, extra);
  cuda_driver_check_error(result, CUDA_SYMBOL_STRING(cuLaunchKernel));
  --outstanding_kernels;
  return result;
}


// /*
//  * Memory copy functions can affect the resident pages on GPU, so we must
//  * block them as well when the client doesn't have the GPU lock.
//  */
// CUresult cuMemcpy(CUdeviceptr dst, CUdeviceptr src, size_t ByteCount)
// {
// 	CUresult result = CUDA_SUCCESS;


// 	if (real_cuMemcpy == NULL) return CUDA_ERROR_NOT_INITIALIZED;

// 	result = real_cuMemcpy(dst, src, ByteCount);
// 	cuda_driver_check_error(result, CUDA_SYMBOL_STRING(cuMemcpy));

// 	return result;
// }

// CUresult cuMemcpyAsync(CUdeviceptr dst, CUdeviceptr src, size_t ByteCount,
// 	CUstream hStream)
// {
// 	CUresult result = CUDA_SUCCESS;


// 	if (real_cuMemcpyAsync == NULL) return CUDA_ERROR_NOT_INITIALIZED;

// 	result = real_cuMemcpyAsync(dst, src, ByteCount, hStream);
// 	cuda_driver_check_error(result, CUDA_SYMBOL_STRING(cuMemcpyAsync));

// 	return result;
// }

// CUresult cuMemcpyDtoH(void *dstHost, CUdeviceptr srcDevice, size_t ByteCount)
// {
// 	CUresult result = CUDA_SUCCESS;


// 	/* Return immediately if not initialized */
// 	if (real_cuMemcpyDtoH == NULL) return CUDA_ERROR_NOT_INITIALIZED;

// 	result = real_cuMemcpyDtoH(dstHost, srcDevice, ByteCount);
// 	cuda_driver_check_error(result, CUDA_SYMBOL_STRING(cuMemcpyDtoH));

// 	return result;
// }

// CUresult cuMemcpyDtoHAsync(void* dstHost, CUdeviceptr srcDevice,
// 	size_t ByteCount, CUstream hStream)
// {
// 	CUresult result = CUDA_SUCCESS;


// 	/* Return immediately if not initialized */
// 	if (real_cuMemcpyDtoHAsync == NULL) return CUDA_ERROR_NOT_INITIALIZED;

// 	result = real_cuMemcpyDtoHAsync(dstHost, srcDevice, ByteCount, hStream);
// 	cuda_driver_check_error(result, CUDA_SYMBOL_STRING(cuMemcpyDtoHAsync));

// 	return result;
// }

// CUresult cuMemcpyHtoD(CUdeviceptr dstDevice, const void* srcHost,
// 	size_t ByteCount)
// {
// 	CUresult result = CUDA_SUCCESS;


// 	/* Return immediately if not initialized */
// 	if (real_cuMemcpyHtoD == NULL) return CUDA_ERROR_NOT_INITIALIZED;

// 	result = real_cuMemcpyHtoD(dstDevice, srcHost, ByteCount);
// 	cuda_driver_check_error(result, CUDA_SYMBOL_STRING(cuMemcpyHtoD));

// 	return result;
// }

// CUresult cuMemcpyHtoDAsync(CUdeviceptr dstDevice, const void* srcHost,
// 	size_t ByteCount, CUstream hStream)
// {
// 	CUresult result = CUDA_SUCCESS;


// 	/* Return immediately if not initialized */
// 	if (real_cuMemcpyHtoDAsync == NULL) return CUDA_ERROR_NOT_INITIALIZED;

// 	result = real_cuMemcpyHtoDAsync(dstDevice, srcHost, ByteCount, hStream);
// 	cuda_driver_check_error(result, CUDA_SYMBOL_STRING(cuMemcpyHtoDAsync));

// 	return result;
// }

// CUresult cuMemcpyDtoD(CUdeviceptr dstDevice, CUdeviceptr srcDevice,
// 	size_t ByteCount)
// {
// 	CUresult result = CUDA_SUCCESS;


// 	/* Return immediately if not initialized */
// 	if (real_cuMemcpyDtoD == NULL) return CUDA_ERROR_NOT_INITIALIZED;

// 	result = real_cuMemcpyDtoD(dstDevice, srcDevice, ByteCount);
// 	cuda_driver_check_error(result, CUDA_SYMBOL_STRING(cuMemcpyDtoD));

// 	return result;
// }

// CUresult cuMemcpyDtoDAsync(CUdeviceptr dstDevice, CUdeviceptr srcDevice,
// 	size_t ByteCount, CUstream hStream)
// {
// 	CUresult result = CUDA_SUCCESS;


// 	/* Return immediately if not initialized */
// 	if (real_cuMemcpyDtoDAsync == NULL) return CUDA_ERROR_NOT_INITIALIZED;

// 	result = real_cuMemcpyDtoDAsync(dstDevice, srcDevice, ByteCount, hStream);
// 	cuda_driver_check_error(result, CUDA_SYMBOL_STRING(cuMemcpyDtoDAsync));

// 	return result;
// }

__asm__(".symver dlsym_225, dlsym@@GLIBC_2.2.5");
__asm__(".symver dlsym_231, dlsym@GLIBC_2.31");
__asm__(".symver dlsym_234, dlsym@GLIBC_2.34");

