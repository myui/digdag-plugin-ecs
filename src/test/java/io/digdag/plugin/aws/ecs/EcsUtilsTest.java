/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.digdag.plugin.aws.ecs;

import static java.nio.charset.StandardCharsets.UTF_8;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import com.amazonaws.services.ecs.model.RegisterTaskDefinitionRequest;
import com.amazonaws.services.ecs.model.RunTaskRequest;

public class EcsUtilsTest {

  @Test
  public void testRunTaskRequest() throws Exception {
    String json = IOUtils.toString(EcsUtils.class.getResourceAsStream("run-task.json"), UTF_8);

    RunTaskRequest request = EcsUtils.unmarshallRunTaskRequest(json);

    Assert.assertEquals(0, request.getCount().intValue());
    Assert.assertEquals("TASK_DEFINITION", request.getPropagateTags());
  }

  @Test
  public void testRegisterTaskDefinitionRequest() throws Exception {
    String json = IOUtils
        .toString(EcsUtils.class.getResourceAsStream("register-task-definition.json"), UTF_8);

    RegisterTaskDefinitionRequest request = EcsUtils.unmarshallRegisterTaskDefinitionRequest(json);

    Assert.assertEquals("none", request.getNetworkMode());
    Assert.assertEquals("task", request.getIpcMode());
  }

}
