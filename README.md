<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

![Iceberg](https://iceberg.apache.org/assets/images/Iceberg-logo.svg)

This is a proof-of-concept implementation of integrating geospatial support to
Apache Iceberg in a Spark geospatial library agnostic manner. Please refer to
[this
documentation](https://wherobots.notion.site/EXT-Geometry-encoding-type-and-expression-in-Iceberg-193f84ff42b44d2db326dc43f753598f#9da2d46063a946fcafa9a4645517e236)
for details.

There is a proof-of-concept connector for Apache Spark implemented in
[wherobots/sedona-iceberg-connector](https://github.com/wherobots/sedona-iceberg-connector).
