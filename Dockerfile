##
# Copyright 2023 Aiven Oy
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##
FROM ivanyuaiven/kafka:3.3-2022-10-06-tiered-storage-1

COPY commons/build/distributions/commons-0.0.1-SNAPSHOT.tar /ursm/commons-0.0.1-SNAPSHOT.tar
RUN cd /ursm \
    && tar -xf commons-0.0.1-SNAPSHOT.tar --strip-components=1 \
    && rm commons-0.0.1-SNAPSHOT.tar

ENV CLASSPATH=$CLASSPATH:/ursm
