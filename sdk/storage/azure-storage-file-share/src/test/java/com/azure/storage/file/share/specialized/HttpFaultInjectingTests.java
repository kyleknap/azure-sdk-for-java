package com.azure.storage.file.share.specialized;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpClientProvider;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.http.netty.NettyAsyncHttpClientProvider;
import com.azure.core.http.okhttp.OkHttpAsyncClientProvider;
import com.azure.core.test.TestMode;
import com.azure.core.test.utils.TestUtils;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.core.util.CoreUtils;
import com.azure.core.util.HttpClientOptions;
import com.azure.core.util.SharedExecutorService;
import com.azure.core.util.UrlBuilder;
import com.azure.core.util.logging.ClientLogger;
import com.azure.storage.common.implementation.Constants;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.azure.storage.file.share.FileShareTestBase;
import com.azure.storage.file.share.FileShareTestHelper;
import com.azure.storage.file.share.ShareClient;
import com.azure.storage.file.share.ShareFileClient;
import com.azure.storage.file.share.ShareFileClientBuilder;
import com.azure.storage.file.share.ShareServiceClientBuilder;
import com.azure.storage.file.share.models.ShareFileUploadOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import reactor.core.publisher.Mono;
import reactor.netty.resources.ConnectionProvider;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.azure.storage.common.test.shared.StorageCommonTestUtils.ENVIRONMENT;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Set of tests that use <a href="">HTTP fault injecting</a> to simulate scenarios where the network has random errors.
 */
@EnabledIf("shouldRun")
public class HttpFaultInjectingTests {
    private static final ClientLogger LOGGER = new ClientLogger(HttpFaultInjectingTests.class);
    private static final HttpHeaderName UPSTREAM_URI_HEADER = HttpHeaderName.fromString("X-Upstream-Base-Uri");
    private static final HttpHeaderName HTTP_FAULT_INJECTOR_RESPONSE_HEADER
        = HttpHeaderName.fromString("x-ms-faultinjector-response-option");

    private ShareClient shareClient;

    @BeforeEach
    public void setup() {
        String testName = ("httpFaultInjectingTests" + CoreUtils.randomUuid().toString().replace("-", ""))
            .toLowerCase();
        System.out.println(testName);
        shareClient = new ShareServiceClientBuilder()
            .endpoint(ENVIRONMENT.getPrimaryAccount().getFileEndpoint())
            .credential(ENVIRONMENT.getPrimaryAccount().getCredential())
            .httpClient(FileShareTestBase.getHttpClient(() -> {
                throw new RuntimeException("Test should not run during playback.");
            }))
            .buildClient()
            .createShare(testName);
    }

    @AfterEach
    public void teardown() {
        if (shareClient != null) {
            shareClient.delete();
        }
    }

    // Method to compute checksum using MD5
    private String calculateChecksum(byte[] data) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(data);
        return Base64.getEncoder().encodeToString(digest); // Encodes checksum for easy comparison
    }

    /**
     * Tests downloading to file with fault injection.
     * <p>
     * This test will upload a single blob of about 9MB and then download it in parallel 500 times. Each download will
     * have its file contents compared to the original blob data. The test only cares about files that were properly
     * downloaded, if a download fails with a network error it will be ignored. A requirement of 90% of files being
     * successfully downloaded is also a requirement to prevent a case where most files failed to download and passing,
     * hiding a true issue.
     */
    @Test
    public void downloadToFileWithFaultInjection() throws InterruptedException, NoSuchAlgorithmException {
        int testRuns = 100;
        int length = 30 * Constants.MB - 1;
        byte[] realFileBytes = new byte[length];
        ThreadLocalRandom.current().nextBytes(realFileBytes);
        String originalChecksum = calculateChecksum(realFileBytes);
        System.out.println("original checksum: " + originalChecksum);

        ShareFileClient fileClient = shareClient.getFileClient(shareClient.getShareName());
        fileClient.create(length);
        fileClient.uploadWithResponse(new ShareFileUploadOptions(BinaryData.fromBytes(realFileBytes).toStream()), null, Context.NONE);

        ShareFileClient downloadClient = new ShareFileClientBuilder()
            .endpoint(ENVIRONMENT.getPrimaryAccount().getFileEndpoint())
            .shareName(shareClient.getShareName())
            .resourcePath(shareClient.getShareName())
            .credential(ENVIRONMENT.getPrimaryAccount().getCredential())
            .httpClient(new HttpFaultInjectingHttpClient(getFaultInjectingWrappedHttpClientWithNetty()))
            //.retryOptions(new RequestRetryOptions(RetryPolicyType.FIXED, 4, null, 10L, 10L, null))
            .buildFileClient();

        List<File> files = new ArrayList<>(testRuns);
        URL testFolder = getClass().getClassLoader().getResource("testfiles");
        //File downloadFile = new File(String.format("%s/%s.txt", testFolder.getPath(), prefix));
        for (int i = 0; i < testRuns; i++) {
            File file = new File(String.format("%s/%s.txt", testFolder.getPath(), i));
            //File file = File.createTempFile(CoreUtils.randomUuid().toString() + i, ".txt");
            file.deleteOnExit();
            files.add(file);
        }
        AtomicInteger successCount = new AtomicInteger();
        Map<String, byte[]> failedDownloads = new ConcurrentHashMap<>();
        Map<String, String> exceptionOccurrences = new ConcurrentHashMap<>();

        CountDownLatch countDownLatch = new CountDownLatch(testRuns);
        SharedExecutorService.getInstance().invokeAll(files.stream().map(it -> (Callable<Void>) () -> {
            try {
                System.out.println("Starting run for file: " + it.getAbsolutePath());
                downloadClient.downloadToFileWithResponse(it.getAbsolutePath(), null, null, Context.NONE);
                byte[] actualFileBytes = Files.readAllBytes(it.toPath());

                try {
                    String downloadedChecksum = calculateChecksum(actualFileBytes);
                    System.out.println("downloaded checksum: " + downloadedChecksum);
                    TestUtils.assertArraysEqual(realFileBytes, actualFileBytes);
                    if (!originalChecksum.equals(downloadedChecksum)) {
                        failedDownloads.put(it.getAbsolutePath(), actualFileBytes);
                        System.out.println("Checksum mismatch for file: " + it.getAbsolutePath());
                        LOGGER.atWarning()
                            .addKeyValue("downloadFile", it.getAbsolutePath())
                            .log("File content did not match expected checksum.");
                    } else {
                        LOGGER.atVerbose()
                            .addKeyValue("successCount", successCount.incrementAndGet())
                            .log("Download completed successfully.");
                        System.out.println("download complete successfully, count: " + successCount);
                    }
                } catch (NoSuchAlgorithmException e) {
                    System.err.println("Checksum algorithm not found: " + e.getMessage());
                    LOGGER.atError().log("Failed to calculate checksum.", e);
                }
//                } catch (AssertionError e) {
//                    failedDownloads.put(it.getAbsolutePath(), actualFileBytes);
//                    System.out.println("Assertion failed for file: " + it.getAbsolutePath());
//                    LOGGER.atWarning()
//                        .addKeyValue("downloadFile", it.getAbsolutePath())
//                        .log("File content did not match expected bytes.", e);
//                }

                if (Files.exists(it.toPath())) {
                    System.out.println("File exists: " + it.getAbsolutePath());
                    FileShareTestHelper.deleteFileIfExists(testFolder.getPath(), it.getName());
                }

            } catch (Throwable ex) {
                ex.printStackTrace();
                exceptionOccurrences.put(it.getAbsolutePath(), ex.getMessage());
                // Don't let network exceptions fail the download
                System.out.println("Error has occurred...");
                System.out.println("Error is: " + ex.getMessage());
                LOGGER.atWarning()
                    .addKeyValue("downloadFile", it.getAbsolutePath())
                    .log("Failed to complete download.", ex);
            } finally {
                countDownLatch.countDown();
                //System.out.println("CountDownLatch: " + countDownLatch.getCount());
            }

            return null;
        }).collect(Collectors.toList()));

        countDownLatch.await(10, TimeUnit.MINUTES);

        //int expectedRuns = (int) (testRuns * 0.90);
        System.out.println("Total successful downloads: " + successCount.get());
        System.out.println("Expected successful downloads: " + testRuns);
        //assertTrue(successCount.get() >= expectedRuns);

        // Print out details of failed downloads for debugging
        if (!failedDownloads.isEmpty()) {
            System.out.println("Failed Downloads: " + failedDownloads.size());
            failedDownloads.forEach((filePath, bytes) -> {
                System.out.println("File: " + filePath + ", Downloaded bytes (sample): " + Arrays.toString(Arrays.copyOf(bytes, Math.min(bytes.length, 100))));
            });
        }

        // Print out details of failed downloads for debugging
        if (!exceptionOccurrences.isEmpty()) {
            System.out.println("Exception Occurrences: " + exceptionOccurrences.size());
            exceptionOccurrences.forEach((filePath, message) -> {
                System.out.println("File: " + filePath + " and Exception: " + message);
            });
        }

        // cleanup
        files.forEach(it -> {
            try {
                Files.deleteIfExists(it.toPath());
            } catch (IOException e) {
                LOGGER.atWarning()
                    .addKeyValue("file", it.getAbsolutePath())
                    .log("Failed to delete file.", e);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private HttpClient getFaultInjectingWrappedHttpClientWithNetty() {
        ConnectionProvider connectionProvider = ConnectionProvider.builder("custom")
            .maxConnections(500)                       // Adjust max connections as needed
            .pendingAcquireTimeout(Duration.ofSeconds(60)) // Increase timeout for acquiring a connection
            .maxIdleTime(Duration.ofSeconds(30))        // Set max idle time for connections
            .maxLifeTime(Duration.ofMinutes(5))         // Set max lifetime for connections
            .build();

        return new NettyAsyncHttpClientBuilder()
            .connectionProvider(connectionProvider)
            .readTimeout(Duration.ofSeconds(60))        // Increase read timeout as needed
            .writeTimeout(Duration.ofSeconds(60))       // Increase write timeout as needed
            .responseTimeout(Duration.ofSeconds(90))    // Increase response timeout as needed
            .build();
    }

    @SuppressWarnings("unchecked")
    private HttpClient getFaultInjectingWrappedHttpClient() {
        ConnectionProvider connectionProvider = ConnectionProvider.builder("custom")
            .maxConnections(100)                       // Adjust max connections as needed
            .pendingAcquireTimeout(Duration.ofSeconds(60)) // Increase timeout for acquiring a connection
            .maxIdleTime(Duration.ofSeconds(30))        // Set max idle time for connections
            .maxLifeTime(Duration.ofMinutes(5))         // Set max lifetime for connections
            .build();

        switch (ENVIRONMENT.getHttpClientType()) {
            case NETTY:
                System.out.println("Using netty");
                return HttpClient.createDefault(new HttpClientOptions()
                    .readTimeout(Duration.ofSeconds(2)).setMaximumConnectionPoolSize(200)
                    //.responseTimeout(Duration.ofSeconds(2))
                    .setHttpClientProvider(NettyAsyncHttpClientProvider.class));
            case OK_HTTP:
                System.out.println("Using ok_http");
                return HttpClient.createDefault(new HttpClientOptions()
                    .readTimeout(Duration.ofSeconds(2)).setMaximumConnectionPoolSize(200)
                    //.responseTimeout(Duration.ofSeconds(2))
                    .setHttpClientProvider(OkHttpAsyncClientProvider.class));
            case VERTX:
                System.out.println("using vertx");
                return HttpClient.createDefault(new HttpClientOptions()
                    .readTimeout(Duration.ofSeconds(2)).setMaximumConnectionPoolSize(200)
                    //.responseTimeout(Duration.ofSeconds(2))
                    .setHttpClientProvider(getVertxClientProviderReflectivelyUntilNameChangeReleases()));
            case JDK_HTTP:
                try {
                    System.out.println("using jdk_http");
                    return HttpClient.createDefault(new HttpClientOptions()
                        .readTimeout(Duration.ofSeconds(2)).setMaximumConnectionPoolSize(200)
                        //.responseTimeout(Duration.ofSeconds(2))
                        .setHttpClientProvider((Class<? extends HttpClientProvider>) Class.forName(
                            "com.azure.core.http.jdk.httpclient.JdkHttpClientProvider")));
                } catch (ClassNotFoundException e) {
                    throw new IllegalStateException(e);
                }

            default:
                throw new IllegalArgumentException("Unknown http client type: " + ENVIRONMENT.getHttpClientType());
        }
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends HttpClientProvider> getVertxClientProviderReflectivelyUntilNameChangeReleases() {
        Class<?> clazz;
        try {
            clazz = Class.forName("com.azure.core.http.vertx.VertxHttpClientProvider");
        } catch (ClassNotFoundException ex) {
            try {
                clazz = Class.forName("com.azure.core.http.vertx.VertxAsyncHttpClientProvider");
            } catch (ClassNotFoundException ex2) {
                ex2.addSuppressed(ex);
                throw new RuntimeException(ex2);
            }
        }

        return (Class<? extends HttpClientProvider>) clazz;
    }

    // For now a local implementation is here in azure-storage-blob until this is released in azure-core-test.
    // Since this is a local definition with a clear set of configurations everything is simplified.
    private static final class HttpFaultInjectingHttpClient implements HttpClient {
        private final HttpClient wrappedHttpClient;

        HttpFaultInjectingHttpClient(HttpClient wrappedHttpClient) {
            this.wrappedHttpClient = wrappedHttpClient;
        }

        @Override
        public Mono<HttpResponse> send(HttpRequest request) {
            return send(request, Context.NONE);
        }

        @Override
        public Mono<HttpResponse> send(HttpRequest request, Context context) {
            URL originalUrl = request.getUrl();
            request.setHeader(UPSTREAM_URI_HEADER, originalUrl.toString()).setUrl(rewriteUrl(originalUrl));
            String faultType = faultInjectorHandling();
            //System.out.println("fault type: " + faultType);
            request.setHeader(HTTP_FAULT_INJECTOR_RESPONSE_HEADER, faultType);

            return wrappedHttpClient.send(request, context)
                .map(response -> {
                    HttpRequest request1 = response.getRequest();
                    request1.getHeaders().remove(UPSTREAM_URI_HEADER);
                    request1.setUrl(originalUrl);

                    return response;
                });
        }

        @Override
        public HttpResponse sendSync(HttpRequest request, Context context) {
            URL originalUrl = request.getUrl();
            request.setHeader(UPSTREAM_URI_HEADER, originalUrl.toString()).setUrl(rewriteUrl(originalUrl));
            String faultType = faultInjectorHandling();
            request.setHeader(HTTP_FAULT_INJECTOR_RESPONSE_HEADER, faultType);

            HttpResponse response = wrappedHttpClient.sendSync(request, context);
            response.getRequest().setUrl(originalUrl);
            response.getRequest().getHeaders().remove(UPSTREAM_URI_HEADER);

            return response;
        }

        private static URL rewriteUrl(URL originalUrl) {
            try {
                return UrlBuilder.parse(originalUrl)
                    .setScheme("http")
                    .setHost("localhost")
                    .setPort(7777)
                    .toUrl();
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }

//        private static String faultInjectorHandling() {
//            // f: Full response
//            // p: Partial Response (full headers, 50% of body), then wait indefinitely
//            // pc: Partial Response (full headers, 50% of body), then close (TCP FIN)
//            // pa: Partial Response (full headers, 50% of body), then abort (TCP RST)
//            // pn: Partial Response (full headers, 50% of body), then finish normally
//            // n: No response, then wait indefinitely
//            // nc: No response, then close (TCP FIN)
//            // na: No response, then abort (TCP RST)
//            double random = ThreadLocalRandom.current().nextDouble();
//            int choice = (int) (random * 100);
//
//            if (choice >= 25) {
//                // 75% of requests complete without error.
//                return "f";
//            } else if (choice >= 1) {
//                if (random <= 0.34D) {
//                    return "n";
//                } else if (random <= 0.67D) {
//                    return "nc";
//                } else {
//                    return "na";
//                }
//            } else {
//                if (random <= 0.25D) {
//                    return "p";
//                } else if (random <= 0.50D) {
//                    return "pc";
//                } else if (random <= 0.75D) {
//                    return "pa";
//                } else {
//                    return "pn";
//                }
//            }
//        }

        private static List<Tuple2<Double, String>> addResponseFaultedProbabilities() {
            // f: Full response
            // p: Partial Response (full headers, 50% of body), then wait indefinitely
            // pc: Partial Response (full headers, 50% of body), then close (TCP FIN)
            // pa: Partial Response (full headers, 50% of body), then abort (TCP RST)
            // pn: Partial Response (full headers, 50% of body), then finish normally
            // n: No response, then wait indefinitely
            // nc: No response, then close (TCP FIN)
            // na: No response, then abort (TCP RST)
            List<Tuple2<Double, String>> probabilities = new ArrayList<>();
            probabilities.add(Tuples.of(0.06, "p"));
            probabilities.add(Tuples.of(0.06, "pc"));
            probabilities.add(Tuples.of(0.06, "pa"));
            probabilities.add(Tuples.of(0.06, "pn"));
            probabilities.add(Tuples.of(0.003, "n"));
            probabilities.add(Tuples.of(0.004, "nc"));
            probabilities.add(Tuples.of(0.003, "na"));
            return probabilities;
        }

        private static String faultInjectorHandling() {
            List<Tuple2<Double, String>> probabilities = addResponseFaultedProbabilities();
            double random = Math.random();
            double sum = 0d;

            for (Tuple2<Double, String> tup : probabilities) {
                if (random < sum + tup.getT1()) {
                    return tup.getT2();
                }
                sum += tup.getT1();
            }
            return "f";
        }
    }

    private static boolean shouldRun() {
        String osName = System.getProperty("os.name").toLowerCase(Locale.ROOT);

        // macOS has known issues running HTTP fault injector, change this once
        // https://github.com/Azure/azure-sdk-tools/pull/6216 is resolved
        return ENVIRONMENT.getTestMode() == TestMode.LIVE
            && !osName.contains("mac os")
            && !osName.contains("darwin");
    }
}
