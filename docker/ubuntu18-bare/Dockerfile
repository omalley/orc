# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ORC compile for Ubuntu 18
#

FROM ubuntu:18.04
MAINTAINER ORC Dev <dev@orc.apache.org>

RUN ln -fs /usr/share/zoneinfo/America/Los_Angeles /etc/localtime
RUN apt-get update
RUN apt-get install -y \
  autoconf \
  autogen \
  automake \
  bison \
  cmake \
  gcc \
  g++ \
  git \
  libtool \
  make \
  maven \
  openjdk-8-jdk \
  tzdata
RUN update-java-alternatives --set java-1.8.0-openjdk-amd64
RUN adduser orc << EOF\n\
orc\norc\n\n\n\n\n\nY\nEOF

WORKDIR /home/orc
VOLUME /home/orc/.m2/repository
USER orc

CMD git clone https://github.com/apache/orc.git -b master && \
  mkdir orc/build && \
  cd orc/build && \
  cmake .. && \
  make package test-out
