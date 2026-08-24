package ro.mig.common;

import java.util.List;

/**
 * The File Storage boundary from architecture diagram.
 *
 * <p>
 * This is one of the adapter seams: locally it is backed by fake-gcs-server
 * over the
 * GCS JSON API; on real GCP the same interface is implemented by the
 * google-cloud-storage
 * client with ADC credentials. Nothing above this interface knows which it is
 * talking to.
 */
public interface ObjectStore {

    void createBucket(String bucket);

    void put(String bucket, String objectName, byte[] content);

    byte[] get(String bucket, String objectName);

    List<String> list(String bucket, String prefix);

    boolean exists(String bucket, String objectName);
}
