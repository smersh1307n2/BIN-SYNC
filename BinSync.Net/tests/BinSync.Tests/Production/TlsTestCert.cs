using System.Net;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace BinSync.Tests;

/// <summary>Test-only helpers for ephemeral TLS certificates.</summary>
internal static class TlsTestCert
{
    internal static X509Certificate2 CreateEphemeral(string cn = "localhost")
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest($"CN={cn}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        req.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        req.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        var eku = new OidCollection { new Oid("1.3.6.1.5.5.7.3.1") }; // TLS Web Server Authentication
        req.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(eku, critical: false));
        req.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(req.PublicKey, false));
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName(cn);
        san.AddDnsName("localhost");
        san.AddIpAddress(IPAddress.Loopback);
        san.AddIpAddress(IPAddress.IPv6Loopback);
        req.CertificateExtensions.Add(san.Build());
        var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(1));
        X509KeyStorageFlags keyStorageFlags = X509KeyStorageFlags.Exportable |
            (OperatingSystem.IsWindows()
                ? X509KeyStorageFlags.UserKeySet
                : X509KeyStorageFlags.EphemeralKeySet);
        return X509CertificateLoader.LoadPkcs12(
            cert.Export(X509ContentType.Pfx), password: null, keyStorageFlags);
    }

    internal static SslClientAuthenticationOptions ClientOptions(string targetHost = "localhost", string? expectedThumbprint = null) => new()
    {
        TargetHost = targetHost,
        EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
        RemoteCertificateValidationCallback = (sender, cert, chain, errors) => cert is not null &&
            (expectedThumbprint is null || string.Equals(cert.GetCertHashString(), expectedThumbprint, StringComparison.OrdinalIgnoreCase)),
    };
}
