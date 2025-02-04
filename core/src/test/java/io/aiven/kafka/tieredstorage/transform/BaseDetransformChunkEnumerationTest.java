/*
 * Copyright 2023 Aiven Oy
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

package io.aiven.kafka.tieredstorage.transform;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import io.aiven.kafka.tieredstorage.Chunk;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BaseDetransformChunkEnumerationTest {
    @Test
    void nullInputStream() {
        assertThatThrownBy(() -> new BaseDetransformChunkEnumeration(null, List.of()))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("inputStream cannot be null");
    }

    @Test
    void nullChunks() {
        assertThatThrownBy(() -> new BaseDetransformChunkEnumeration(InputStream.nullInputStream(), null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("chunks cannot be null");
    }

    @Test
    void inputStreamFewerBytesThanExpected() {
        // 1 byte in the input stream.
        final var bais = new ByteArrayInputStream(new byte[]{0});
        // 2 chunks of 1 byte expected.
        final List<Chunk> chunks = List.of(
            new Chunk(0, 0, 1, 0, 1),
            new Chunk(1, 1, 1, 1, 1)
        );

        final var transform = new BaseDetransformChunkEnumeration(bais, chunks);
        assertThat(transform.hasMoreElements()).isTrue();
        transform.nextElement();
        assertThatThrownBy(() -> transform.hasMoreElements())
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Stream has fewer bytes than expected");
    }

    @Test
    void inputStreamMoreBytesThanExpected() {
        // 16 bytes in the input stream.
        final byte[] data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        testOneChunk(data, 10, Arrays.copyOfRange(data, 0, 10));
    }

    @Test
    void inOneChunk() {
        // 10 bytes in the input stream.
        final byte[] data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        testOneChunk(data, 10, data);
    }

    void testOneChunk(final byte[] originalData, final int expectedChunkSize, final byte[] expectedData) {
        // 1 chunk is expected.
        final List<Chunk> chunks = List.of(
            new Chunk(0, 0, expectedChunkSize, 0, expectedChunkSize)
        );
        final var transform = new BaseDetransformChunkEnumeration(new ByteArrayInputStream(originalData), chunks);
        assertThat(transform.hasMoreElements()).isTrue();
        assertThat(transform.nextElement()).isEqualTo(expectedData);

        assertThat(transform.hasMoreElements()).isFalse();
        assertThatThrownBy(transform::nextElement)
            .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void inManyChunks() {
        // 10 bytes in the input stream.
        final byte[] data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        // 4 chunks of 3, 3, 3, and 1bytes expected.
        final List<Chunk> chunks = List.of(
            new Chunk(0, 0, 3, 0, 3),
            new Chunk(1, 3, 3, 3, 3),
            new Chunk(2, 6, 3, 6, 3),
            new Chunk(3, 9, 3, 9, 1)
        );
        final var transform = new BaseDetransformChunkEnumeration(new ByteArrayInputStream(data), chunks);
        assertThat(transform.hasMoreElements()).isTrue();
        assertThat(transform.nextElement()).isEqualTo(new byte[] {0, 1, 2});
        assertThat(transform.hasMoreElements()).isTrue();
        assertThat(transform.nextElement()).isEqualTo(new byte[] {3, 4, 5});
        assertThat(transform.hasMoreElements()).isTrue();
        assertThat(transform.nextElement()).isEqualTo(new byte[] {6, 7, 8});
        assertThat(transform.hasMoreElements()).isTrue();
        assertThat(transform.nextElement()).isEqualTo(new byte[] {9});

        assertThat(transform.hasMoreElements()).isFalse();
        assertThatThrownBy(transform::nextElement)
            .isInstanceOf(NoSuchElementException.class);
    }
}
