package ro.mig.common;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * The "archiving -&gt; compression" half of the extraction lane's file processing.
 *
 * <p>tar for archiving (grouping the {@code .DAT}/{@code .CHS}/{@code .ERR}/{@code .RPT} set into
 * one transport unit) and gzip for compression, matching the spec's ordering. The {@code .FLG}
 * semaphore stays <em>outside</em> the bundle so a watcher can detect completion without
 * decrypting anything.
 */
public final class Archives {

    private Archives() {
    }

    public static byte[] tarGz(Map<String, byte[]> files) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out);
             TarArchiveOutputStream tar = new TarArchiveOutputStream(gzip)) {
            tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
            for (Map.Entry<String, byte[]> file : files.entrySet()) {
                TarArchiveEntry entry = new TarArchiveEntry(file.getKey());
                entry.setSize(file.getValue().length);
                tar.putArchiveEntry(entry);
                tar.write(file.getValue());
                tar.closeArchiveEntry();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("could not build tar.gz bundle", e);
        }
        return out.toByteArray();
    }

    public static Map<String, byte[]> unTarGz(byte[] bundle) {
        Map<String, byte[]> files = new LinkedHashMap<>();
        try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(bundle));
             TarArchiveInputStream tar = new TarArchiveInputStream(gzip)) {
            TarArchiveEntry entry;
            while ((entry = tar.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                ByteArrayOutputStream content = new ByteArrayOutputStream();
                tar.transferTo(content);
                files.put(entry.getName(), content.toByteArray());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("could not read tar.gz bundle", e);
        }
        return files;
    }
}
