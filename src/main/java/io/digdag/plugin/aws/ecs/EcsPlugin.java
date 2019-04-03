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

import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorProvider;
import io.digdag.spi.Plugin;
import io.digdag.spi.TemplateEngine;
import java.util.Arrays;
import java.util.List;
import javax.inject.Inject;

public class EcsPlugin implements Plugin {
  @Override
  public <T> Class<? extends T> getServiceProvider(Class<T> type) {
    if (type == OperatorProvider.class) {
      return EcsOperatorProvider.class.asSubclass(type);
    } else {
      return null;
    }
  }

  public static class EcsOperatorProvider implements OperatorProvider {
    @Inject
    protected TemplateEngine templateEngine;

    @Override
    public List<OperatorFactory> get() {
      return Arrays.asList(new EcsRegisterOperatorFactory(templateEngine),
          new EcsRunOperatorFactory());
    }
  }
}
