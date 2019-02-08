/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.digdag.plugin.ecs;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;

import java.io.IOException;
import java.nio.file.Files;

import javax.annotation.Nonnull;

import com.google.common.base.Throwables;

public final class EcsRegisterOperatorFactory implements OperatorFactory {

    @Nonnull
    private final TemplateEngine templateEngine;

    public EcsRegisterOperatorFactory(@Nonnull TemplateEngine templateEngine) {
        this.templateEngine = templateEngine;
    }

    public String getType() {
        return "ecs_register";
    }

    @Override
    public Operator newOperator(@Nonnull OperatorContext context) {
        return new RegisterOperator(context);
    }

    private class RegisterOperator extends EcsBaseOperator {

        public RegisterOperator(@Nonnull OperatorContext context) {
            super(context);
        }

        @Override
        public TaskResult runTask() {
            Config params = request.getConfig()
                                   .mergeDefault(request.getConfig().getNestedOrGetEmpty("ecs"));

            String message = workspace.templateCommand(templateEngine, params, "message", UTF_8);
            String path = params.get("path", String.class);

            try {
                Files.write(workspace.getPath(path), message.getBytes(UTF_8));
            } catch (IOException ex) {
                throw Throwables.propagate(ex);
            }

            return TaskResult.empty(request);
        }
    }

}
