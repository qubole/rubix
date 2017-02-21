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
package com.qubole.rubix.core;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class ReadRequestFactory
        extends BasePooledObjectFactory<ReadRequest>
{
    @Override
    public ReadRequest create()
    {
        return new ReadRequest();
    }

    /**
     * Use the default PooledObject implementation.
     */
    @Override
    public PooledObject<ReadRequest> wrap(ReadRequest readRequest)
    {
        return new DefaultPooledObject<ReadRequest>(readRequest);
    }

    /**
     * When an object is returned to the pool, clear the buffer.
     */
    @Override
    public void passivateObject(PooledObject<ReadRequest> pooledObject)
    {
//        pooledObject.getObject().setLength(0);
    }
    // for all other methods, the no-op implementation
    // in BasePooledObjectFactory will suffice
}
