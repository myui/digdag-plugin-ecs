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
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
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
    AWSCredentialsProvider credentials = credentials();
    ClientConfiguration ecsClientConfiguration =
        new ClientConfiguration().withProtocol(Protocol.HTTPS);

    AmazonECSClientBuilder clientBuilder = AmazonECSClient.builder().withCredentials(credentials)
        .withClientConfiguration(ecsClientConfiguration);

    clientBuilder = configureEcsClientBuilder(clientBuilder, config);

    AmazonECSClient client = (AmazonECSClient) clientBuilder.build();
    return client;
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
    } else {
      clientBuilder = clientBuilder.withRegion(Regions.US_EAST_1);
    }
    return clientBuilder;
  }

  @Nonnull
  protected AWSCredentialsProvider credentials() {
    String tag = state.constant("tag", String.class, EcsBaseOperator::randomTag);
    return credentials(tag);
  }

  @Nonnull
  protected AWSCredentialsProvider credentials(@Nonnull String tag) {
    // 1. if profile is specified, use it
    SecretProvider awsSecrets = context.getSecrets().getSecrets("aws");
    Optional<String> profile = awsSecrets.getSecretOptional("profile");
    if (profile.isPresent()) {
      return new ProfileCredentialsProvider(profile.get());
    }

    // 2. try access key and secret
    SecretProvider ecsSecrets = awsSecrets.getSecrets("ecs");
    Optional<String> accessKeyId = first(() -> ecsSecrets.getSecretOptional("access_key_id"),
        () -> awsSecrets.getSecretOptional("access_key_id"));
    Optional<String> secretAccessKey =
        first(() -> ecsSecrets.getSecretOptional("secret_access_key"),
            () -> awsSecrets.getSecretOptional("secret_access_key"));
    if (accessKeyId.isPresent() && secretAccessKey.isPresent()) {
      return new AWSStaticCredentialsProvider(
          new BasicAWSCredentials(accessKeyId.get(), secretAccessKey.get()));
    }

    // 3. fall back to default
    return new DefaultAWSCredentialsProviderChain();
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
