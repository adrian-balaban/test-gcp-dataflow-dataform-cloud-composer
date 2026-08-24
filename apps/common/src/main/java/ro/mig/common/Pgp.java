package ro.mig.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * PGP encryption for the extraction lane ("archiving -&gt; compression -&gt; PGP encryption").
 *
 * <p>Deliberately shells out to the system {@code gpg} binary rather than embedding an OpenPGP
 * library. Two reasons: it is what mainframe-adjacent batch estates actually do, and it
 * guarantees byte-level interoperability with the Python side of the pipeline, which decrypts
 * with the same binary. A cross-library BouncyCastle/GnuPG pairing would be an interop risk the
 * prototype does not need to take.
 *
 * <p><b>On real GCP</b> the private key never lands on a worker: it lives in Secret Manager and
 * is streamed into a transient keyring, or decryption moves behind a KMS-backed service. See
 * docs/runbook-gcp.md.
 */
public final class Pgp {

    private final Path gnupgHome;

    public Pgp(Path gnupgHome) {
        this.gnupgHome = gnupgHome.toAbsolutePath();
    }

    public byte[] encrypt(byte[] plaintext, String recipient) {
        return run(plaintext, List.of(
                "--batch", "--yes", "--trust-model", "always",
                "--recipient", recipient, "--encrypt", "--output", "-"));
    }

    public byte[] decrypt(byte[] ciphertext) {
        return run(ciphertext, List.of(
                "--batch", "--yes", "--pinentry-mode", "loopback", "--passphrase", "",
                "--decrypt", "--output", "-"));
    }

    private byte[] run(byte[] input, List<String> args) {
        List<String> command = new ArrayList<>();
        command.add("gpg");
        command.add("--homedir");
        command.add(gnupgHome.toString());
        command.addAll(args);

        Process process;
        try {
            process = new ProcessBuilder(command).redirectErrorStream(false).start();
        } catch (IOException e) {
            throw new PgpException("could not start gpg — is GnuPG installed?", e);
        }

        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();

        Thread outPump = pump(process.getInputStream(), stdout);
        Thread errPump = pump(process.getErrorStream(), stderr);

        try (OutputStream stdin = process.getOutputStream()) {
            stdin.write(input);
        } catch (IOException e) {
            throw new PgpException("could not write plaintext to gpg", e);
        }

        try {
            if (!process.waitFor(120, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new PgpException("gpg timed out after 120s", null);
            }
            outPump.join(10_000);
            errPump.join(10_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PgpException("interrupted while waiting for gpg", e);
        }

        if (process.exitValue() != 0) {
            throw new PgpException(
                    "gpg exited " + process.exitValue() + ": " + stderr.toString().trim(), null);
        }
        return stdout.toByteArray();
    }

    private static Thread pump(InputStream from, ByteArrayOutputStream to) {
        Thread t = new Thread(() -> {
            try (InputStream in = from) {
                in.transferTo(to);
            } catch (IOException ignored) {
                // The process exit code is the authority on success; a broken pipe here is noise.
            }
        });
        t.setDaemon(true);
        t.start();
        return t;
    }

    public static final class PgpException extends RuntimeException {
        public PgpException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
