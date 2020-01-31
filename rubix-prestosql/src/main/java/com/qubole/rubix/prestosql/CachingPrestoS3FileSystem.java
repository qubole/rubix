/**
 * Copyright (c) 2019. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.qubole.rubix.prestosql;

import com.qubole.rubix.core.CachingFileSystem;
import io.prestosql.plugin.hive.s3.PrestoS3FileSystem;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;

/**
 * Created by stagra on 14/1/16.
 */
public class CachingPrestoS3FileSystem extends CachingFileSystem<PrestoS3FileSystem>
{
  public CachingPrestoS3FileSystem()
  {
    super();
  }

  private static final String SCHEME = "s3n";

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException
  {
    super.initialize(uri, conf);
  }

  public String getScheme()
  {
    return SCHEME;
  }
}
