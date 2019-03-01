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

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.SecretProvider;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.util.BaseOperator;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nonnull;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ecs.AmazonECSClient;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.io.BaseEncoding;

public abstract class EcsBaseOperator extends BaseOperator {

  @Nonnull
  protected final TaskState state;

  public EcsBaseOperator(OperatorContext context) {
    super(context);
    this.state = TaskState.of(request);
  }

  @Nonnull
  protected AmazonECSClient getEcsClient(@Nonnull Config config) {
    AWSCredentials credentials = credentials();
    ClientConfiguration ecsClientConfiguration = new ClientConfiguration();

    AmazonECSClientBuilder clientBuilder =
        AmazonECSClient.builder().withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withClientConfiguration(ecsClientConfiguration);

    clientBuilder = configureEcsClientBuilder(clientBuilder, config);

    return (AmazonECSClient) clientBuilder.build();
  }

  @Nonnull
  protected AmazonECSClientBuilder configureEcsClientBuilder(
      @Nonnull AmazonECSClientBuilder clientBuilder, @Nonnull Config config) {
    SecretProvider awsSecrets = context.getSecrets().getSecrets("aws");
    SecretProvider ecsSecrets = awsSecrets.getSecrets("ecs");

    Optional<String> ecsRegionName = first(() -> ecsSecrets.getSecretOptional("region"),
        () -> awsSecrets.getSecretOptional("region"),
        () -> config.getOptional("ecs.region", String.class));

    Optional<String> ecsEndpoint = first(() -> ecsSecrets.getSecretOptional("endpoint"),
        () -> config.getOptional("ecs.endpoint", String.class),
        () -> ecsRegionName.transform(regionName -> "ecs." + regionName + ".amazonaws.com"));

    if (ecsEndpoint.isPresent() && ecsRegionName.isPresent()) {
      clientBuilder = clientBuilder.withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(ecsEndpoint.get(), ecsRegionName.get()));
    } else if (ecsRegionName.isPresent()) {
      final Regions region;
      try {
        region = Regions.fromName(ecsRegionName.get());
      } catch (IllegalArgumentException e) {
        throw new ConfigException("Illegal AWS region: " + ecsRegionName.get());
      }
      clientBuilder = clientBuilder.withRegion(region);
    }
    return clientBuilder;
  }

  @Nonnull
  protected AWSCredentials credentials() {
    String tag = state.constant("tag", String.class, EcsBaseOperator::randomTag);
    return credentials(tag);
  }

  @Nonnull
  protected AWSCredentials credentials(@Nonnull String tag) {
    SecretProvider awsSecrets = context.getSecrets().getSecrets("aws");
    SecretProvider ecsSecrets = awsSecrets.getSecrets("ecs");

    String accessKeyId = ecsSecrets.getSecretOptional("access_key_id")
        .or(() -> awsSecrets.getSecret("access_key_id"));

    String secretAccessKey = ecsSecrets.getSecretOptional("secret_access_key")
        .or(() -> awsSecrets.getSecret("secret_access_key"));

    return new BasicAWSCredentials(accessKeyId, secretAccessKey);
  }

  @Nonnull
  protected static String randomTag() {
    byte[] bytes = new byte[8];
    ThreadLocalRandom.current().nextBytes(bytes);
    return BaseEncoding.base32().omitPadding().encode(bytes);
  }

  @SafeVarargs
  protected static <T> Optional<T> first(Supplier<Optional<T>>... suppliers) {
    for (Supplier<Optional<T>> supplier : suppliers) {
      Optional<T> optional = supplier.get();
      if (optional.isPresent()) {
        return optional;
      }
    }
    return Optional.absent();
  }

}
