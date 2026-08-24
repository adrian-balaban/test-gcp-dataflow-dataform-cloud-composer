package ro.mig.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Bucket provisioning belongs to Terraform on real GCS, and to the app on the emulator.
 *
 * <p>
 * The distinction is not cosmetic: the pods run as the least-privilege
 * {@code dataflow-worker} service account, which has no project-level
 * {@code storage.buckets.create}. Calling it anyway failed the loader task under Composer
 * with a 403 while passing on a laptop, where the human operator's own credentials happen
 * to allow it — so the bug was invisible to every test that ran outside the DAG.
 */
class HttpObjectStoreTest {

    @Test
    void createBucketIsANoOpAgainstRealGcs() {
        // No stub server: reaching the network at all would fail the test, which is the
        // point — against the real endpoint this must return without issuing a request.
        HttpObjectStore store =
                new HttpObjectStore("https://storage.googleapis.com", () -> "fake-token");

        assertDoesNotThrow(() -> store.createBucket("mig-000001-1-dev-recon"));
    }
}
