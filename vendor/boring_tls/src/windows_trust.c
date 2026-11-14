/* Windows owns chain building and policy, including the machine/user stores and disallowed certificates. */
#undef OPENSSL_64_BIT
#include <openssl/ssl.h>
#include <openssl/pool.h>
#include <windows.h>
#include <wincrypt.h>
#include <stdlib.h>

static enum ssl_verify_result_t verify_windows(SSL *ssl, uint8_t *alert) {
    const char *name = SSL_get_ex_data(ssl, 0);
    const STACK_OF(CRYPTO_BUFFER) *chain = SSL_get0_peer_certificates(ssl);
    HCERTSTORE extra = NULL;
    PCCERT_CONTEXT leaf = NULL;
    PCCERT_CHAIN_CONTEXT built = NULL;
    WCHAR *wide = NULL;
    enum ssl_verify_result_t result = ssl_verify_invalid;
    (void)alert;
    if (!name || !chain || sk_CRYPTO_BUFFER_num(chain) == 0) goto done;
    int count = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, name, -1, NULL, 0);
    if (count <= 0) goto done;
    wide = malloc((size_t)count * sizeof(*wide));
    if (!wide || !MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, name, -1, wide, count)) goto done;
    extra = CertOpenStore(CERT_STORE_PROV_MEMORY, 0, 0, CERT_STORE_CREATE_NEW_FLAG, NULL);
    if (!extra) goto done;
    for (size_t i = 0; i < sk_CRYPTO_BUFFER_num(chain); i++) {
        const CRYPTO_BUFFER *cert = sk_CRYPTO_BUFFER_value(chain, i);
        size_t len = CRYPTO_BUFFER_len(cert);
        if (len > MAXDWORD) goto done;
        PCCERT_CONTEXT added = NULL;
        if (!CertAddEncodedCertificateToStore(extra, X509_ASN_ENCODING, CRYPTO_BUFFER_data(cert),
                                               (DWORD)len, CERT_STORE_ADD_ALWAYS, &added)) goto done;
        if (i == 0) leaf = added; else CertFreeCertificateContext(added);
    }
    CERT_CHAIN_PARA params = {0};
    params.cbSize = sizeof(params);
    LPSTR server_auth[] = {szOID_PKIX_KP_SERVER_AUTH};
    params.RequestedUsage.dwType = USAGE_MATCH_TYPE_AND;
    params.RequestedUsage.Usage.cUsageIdentifier = 1;
    params.RequestedUsage.Usage.rgpszUsageIdentifier = server_auth;
    if (!CertGetCertificateChain(NULL, leaf, NULL, extra, &params, 0, NULL, &built)) goto done;
    SSL_EXTRA_CERT_CHAIN_POLICY_PARA ssl_policy = {0};
    ssl_policy.cbSize = sizeof(ssl_policy);
    ssl_policy.dwAuthType = AUTHTYPE_SERVER;
    ssl_policy.pwszServerName = wide;
    CERT_CHAIN_POLICY_PARA policy = {0};
    policy.cbSize = sizeof(policy);
    policy.pvExtraPolicyPara = &ssl_policy;
    CERT_CHAIN_POLICY_STATUS status = {0};
    status.cbSize = sizeof(status);
    if (CertVerifyCertificateChainPolicy(CERT_CHAIN_POLICY_SSL, built, &policy, &status) && status.dwError == 0)
        result = ssl_verify_ok;
done:
    if (built) CertFreeCertificateChain(built);
    if (leaf) CertFreeCertificateContext(leaf);
    if (extra) CertCloseStore(extra, 0);
    free(wide);
    return result;
}

void zpq_windows_trust(SSL_CTX *ctx) {
    SSL_CTX_set_custom_verify(ctx, SSL_VERIFY_PEER, verify_windows);
}
