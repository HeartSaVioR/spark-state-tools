/*
 * Copyright 2019 Jungtaek Lim "<kabhwan@gmail.com>"
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object HadoopPathUtil {
  def resolve(hadoopConf: Configuration, cpLocation: String): String = {
    val checkpointPath = new Path(cpLocation)
    val fs = checkpointPath.getFileSystem(hadoopConf)
    checkpointPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toUri.toString
  }
}
