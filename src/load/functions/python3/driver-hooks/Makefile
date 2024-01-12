# Copyright (c) 2023 Georgios Alexopoulos
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

CC = gcc
GENERAL_LDFLAGS = -Wl,-z,defs -Wl,-z,relro -Wl,-z,now -Wl,--no-undefined
LIBGPUSHARE_LDFLAGS = -shared -Wl,-soname=libgpushare.so -Wl,--version-script=libgpushare-symbols.ld -Wl,--exclude-libs,ALL
LIBGPUSHARE_LDLIBS = -ldl -lpthread
SCHEDULER_LDLIBS = -lpthread
CFLAGS = -O3 -Wall -Wextra -std=gnu99 -fPIC -D_FORTIFY_SOURCE=2 -Werror

# Target rules
all: libgpushare.so 

libgpushare.so: hook.o common.o 
	$(CC) $(GENERAL_LDFLAGS) $(LIBGPUSHARE_LDFLAGS) $^ -o $@ $(LIBGPUSHARE_LDLIBS)

hook.o: hook.c 
	$(CC) $(CFLAGS) $(INCLUDES) -c $^ -o $@ 

common.o: common.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $^ -o $@

clean:
	rm -vf *.o *.so 

