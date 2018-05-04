/**
 * Copyright (c) 2016. Qubole Inc
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
package com.qubole.rubix.presto;

import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 * Created by jordanw on 28/4/18.
 */
@Test(singleThreaded = true)
public class TestCachingPrestoS3FileSystem
{
  @Test
  /*
   * Tests that PrestoS3FileSystem constructor/query methods have coverage.
   */
  public void testBasic()
  {
    final CachingPrestoS3FileSystem s3cacher = new CachingPrestoS3FileSystem();
    final String scheme = s3cacher.getScheme();

    assertTrue(scheme.equals("s3n"), "Scheme must be s3n");
  }
}
