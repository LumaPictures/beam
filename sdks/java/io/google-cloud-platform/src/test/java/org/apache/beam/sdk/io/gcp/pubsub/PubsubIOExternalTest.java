/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.pubsub;

import static org.hamcrest.MatcherAssert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.runners.core.construction.expansion.ExpansionService;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for building {@link PubsubIO} externally via the ExpansionService. */
@RunWith(JUnit4.class)
public class PubsubIOExternalTest {
  @Test
  public void testConstructPubsubRead() throws Exception {
    String topic = "projects/project-1234/topics/topic_name";
    String subscription = "subscription1";
    String idAttribute = "id_foo";
    Long needsAttributes = 1L;

    ExternalTransforms.ExternalConfigurationPayload payload =
        ExternalTransforms.ExternalConfigurationPayload.newBuilder()
            .putConfiguration(
                "topic",
                ExternalTransforms.ConfigValue.newBuilder()
                    .addCoderUrn("beam:coder:bytes:v1")
                    .setPayload(ByteString.copyFrom(encodeString(topic)))
                    .build())
            //            .putConfiguration(
            //                "subscription",
            //                ExternalTransforms.ConfigValue.newBuilder()
            //                    .addCoderUrn("beam:coder:bytes:v1")
            //                    .setPayload(ByteString.copyFrom(encodeString(subscription)))
            //                    .build())
            .putConfiguration(
                "id_label",
                ExternalTransforms.ConfigValue.newBuilder()
                    .addCoderUrn("beam:coder:bytes:v1")
                    .setPayload(ByteString.copyFrom(encodeString(idAttribute)))
                    .build())
            .putConfiguration(
                "with_attributes",
                ExternalTransforms.ConfigValue.newBuilder()
                    .addCoderUrn("beam:coder:varint:v1")
                    .setPayload(ByteString.copyFrom(encodeVarLong(needsAttributes)))
                    .build())
            .build();

    RunnerApi.Components defaultInstance = RunnerApi.Components.getDefaultInstance();
    ExpansionApi.ExpansionRequest request =
        ExpansionApi.ExpansionRequest.newBuilder()
            .setComponents(defaultInstance)
            .setTransform(
                RunnerApi.PTransform.newBuilder()
                    .setUniqueName("test")
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn("beam:external:java:pubsub:read:v1")
                            .setPayload(payload.toByteString())))
            .setNamespace("test_namespace")
            .build();

    ExpansionService expansionService = new ExpansionService();
    TestStreamObserver<ExpansionApi.ExpansionResponse> observer = new TestStreamObserver<>();
    expansionService.expand(request, observer);

    ExpansionApi.ExpansionResponse result = observer.result;
    RunnerApi.PTransform transform = result.getTransform();
    assertThat(
        transform.getSubtransformsList(),
        Matchers.contains(
            "test_namespacetest/PubsubUnboundedSource", "test_namespacetest/MapElements"));
    assertThat(transform.getInputsCount(), Matchers.is(0));
    assertThat(transform.getOutputsCount(), Matchers.is(1));

    RunnerApi.PTransform pubsubComposite =
        result.getComponents().getTransformsOrThrow(transform.getSubtransforms(0));
    RunnerApi.PTransform pubsubRead =
        result.getComponents().getTransformsOrThrow(pubsubComposite.getSubtransforms(0));
    RunnerApi.ReadPayload readPayload =
        RunnerApi.ReadPayload.parseFrom(pubsubRead.getSpec().getPayload());
    PubsubUnboundedSource.PubsubSource source =
        (PubsubUnboundedSource.PubsubSource) ReadTranslation.unboundedSourceFromProto(readPayload);
    PubsubUnboundedSource spec = source.outer;

    assertThat(
        spec.getTopicProvider() == null ? null : String.valueOf(spec.getTopicProvider()),
        Matchers.is(topic));
    assertThat(spec.getIdAttribute(), Matchers.is(idAttribute));
    assertThat(spec.getNeedsAttributes(), Matchers.is(true));
  }

  @Test
  public void testConstructPubsubWrite() throws Exception {
    String topic = "projects/project-1234/topics/topic_name";
    String idAttribute = "id_foo";
    Long needsAttributes = 1L;

    ExternalTransforms.ExternalConfigurationPayload payload =
        ExternalTransforms.ExternalConfigurationPayload.newBuilder()
            .putConfiguration(
                "topic",
                ExternalTransforms.ConfigValue.newBuilder()
                    .addCoderUrn("beam:coder:bytes:v1")
                    .setPayload(ByteString.copyFrom(encodeString(topic)))
                    .build())
            .putConfiguration(
                "id_label",
                ExternalTransforms.ConfigValue.newBuilder()
                    .addCoderUrn("beam:coder:bytes:v1")
                    .setPayload(ByteString.copyFrom(encodeString(idAttribute)))
                    .build())
            //            .putConfiguration(
            //                "with_attributes",
            //                ExternalTransforms.ConfigValue.newBuilder()
            //                    .addCoderUrn("beam:coder:varint:v1")
            //                    .setPayload(ByteString.copyFrom(encodeVarLong(needsAttributes)))
            //                    .build())
            .build();

    Pipeline p = Pipeline.create();
    // FIXME: we have to create a streaming example or we end up with a
    //  PubsubBoundedWriter which does not have the attributes we care about
    p.apply(Impulse.create()).apply(WithKeys.of("key"));

    System.out.println();
    /*
    Source<?> source;
    assumeThat(source, instanceOf(UnboundedSource.class));
    UnboundedSource<?, ?> unboundedSource = (UnboundedSource<?, ?>) source;
    Read.Unbounded<?> unboundedRead = Read.from(unboundedSource);
    */

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    String inputPCollection =
        Iterables.getOnlyElement(
            Iterables.getLast(pipelineProto.getComponents().getTransformsMap().values())
                .getOutputsMap()
                .values());

    ExpansionApi.ExpansionRequest request =
        ExpansionApi.ExpansionRequest.newBuilder()
            .setComponents(pipelineProto.getComponents())
            .setTransform(
                RunnerApi.PTransform.newBuilder()
                    .setUniqueName("test")
                    .putInputs("input", inputPCollection)
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn("beam:external:java:pubsub:write:v1")
                            .setPayload(payload.toByteString())))
            .setNamespace("test_namespace")
            .build();

    ExpansionService expansionService = new ExpansionService();
    TestStreamObserver<ExpansionApi.ExpansionResponse> observer = new TestStreamObserver<>();
    expansionService.expand(request, observer);

    ExpansionApi.ExpansionResponse result = observer.result;
    RunnerApi.PTransform transform = result.getTransform();
    assertThat(
        transform.getSubtransformsList(),
        Matchers.contains("test_namespacetest/ParDo(PubsubBoundedWriter)"));
    assertThat(transform.getInputsCount(), Matchers.is(1));
    assertThat(transform.getOutputsCount(), Matchers.is(0));

    RunnerApi.PTransform writeComposite =
        result.getComponents().getTransformsOrThrow(transform.getSubtransforms(0));

    RunnerApi.PTransform writeParDo =
        result.getComponents().getTransformsOrThrow(writeComposite.getSubtransforms(0));

    RunnerApi.ParDoPayload parDoPayload =
        RunnerApi.ParDoPayload.parseFrom(writeParDo.getSpec().getPayload());
    DoFn pubsubWriter = ParDoTranslation.getDoFn(parDoPayload);
    assertThat(pubsubWriter, Matchers.instanceOf(PubsubIO.Write.PubsubBoundedWriter.class));
    /*
    PubsubIO.WriteRecords spec =
        (PubsubIO.WriteRecords) Whitebox.getInternalState(pubsubWriter, "spec");

    assertThat(
        spec.getTopicProvider() == null ? null : spec.getTopicProvider().get().asPath(),
        Matchers.is(topic));
    assertThat(spec.getIdAttribute(), Matchers.is(idAttribute));
    assertThat(spec.getNeedsAttributes(), Matchers.is(needsAttributes));
    */
  }

  private static byte[] listAsBytes(List<String> stringList) throws IOException {
    IterableCoder<byte[]> coder = IterableCoder.of(ByteArrayCoder.of());
    List<byte[]> bytesList =
        stringList.stream().map(PubsubIOExternalTest::utf8Bytes).collect(Collectors.toList());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    coder.encode(bytesList, baos);
    return baos.toByteArray();
  }

  private static byte[] mapAsBytes(Map<String, String> stringMap) throws IOException {
    IterableCoder<KV<byte[], byte[]>> coder =
        IterableCoder.of(KvCoder.of(ByteArrayCoder.of(), ByteArrayCoder.of()));
    List<KV<byte[], byte[]>> bytesList =
        stringMap.entrySet().stream()
            .map(kv -> KV.of(utf8Bytes(kv.getKey()), utf8Bytes(kv.getValue())))
            .collect(Collectors.toList());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    coder.encode(bytesList, baos);
    return baos.toByteArray();
  }

  private static byte[] encodeString(String str) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ByteArrayCoder.of().encode(utf8Bytes(str), baos);
    return baos.toByteArray();
  }

  private static byte[] encodeVarLong(Long value) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    VarLongCoder.of().encode(value, baos);
    return baos.toByteArray();
  }

  private static @Nullable String getTopic(@Nullable ValueProvider<PubsubIO.PubsubTopic> value) {
    if (value == null) {
      return null;
    }
    return String.valueOf(value);
  }

  private static byte[] utf8Bytes(String str) {
    Preconditions.checkNotNull(str, "String must not be null.");
    return str.getBytes(Charsets.UTF_8);
  }

  private static class TestStreamObserver<T> implements StreamObserver<T> {

    private T result;

    @Override
    public void onNext(T t) {
      result = t;
    }

    @Override
    public void onError(Throwable throwable) {
      throw new RuntimeException("Should not happen", throwable);
    }

    @Override
    public void onCompleted() {}
  }
}
