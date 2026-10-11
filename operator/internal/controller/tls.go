package controller

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	isomerv1 "github.com/Ash20pk/IsomerDB/operator/api/v1alpha1"
)

// Nodes talk to each other over TLS and authenticate with a certificate, not a password: Citus
// connects to workers as each client's own role (a MongoDB user's, say), whose password the
// coordinator does not hold, so the node certificate's CN maps to any role (pg_ident "all").
//
// <name>-ca  holds the cluster's CA (ca.crt, ca.key). It is created once and kept.
// <name>-tls holds what the pods mount: ca.crt, the server certificate (tls.crt, tls.key) for
//            every node's DNS name, and the node client certificate (client.crt, client.key).
//            It is reissued when it is within renewBefore of expiry, or its names change.

const (
	nodeCertCN  = "isomer-node"
	caValidity  = 10 * 365 * 24 * time.Hour
	certValid   = 365 * 24 * time.Hour
	renewBefore = 30 * 24 * time.Hour
	tlsDir      = "/etc/isomerdb/tls"
)

func (n names) caSecret() string  { return n.c.Name + "-ca" }
func (n names) tlsSecret() string { return n.c.Name + "-tls" }

// serverNames are the DNS names a node's server certificate must cover: every pod behind each
// headless service, and the client Service.
func (n names) serverNames() []string {
	ns := n.c.Namespace
	out := []string{}
	for _, svc := range []string{n.coordinator(), n.worker(), n.coordinatorReplica(), n.workerReplica()} {
		out = append(out, fmt.Sprintf("*.%s.%s.svc.cluster.local", svc, ns), fmt.Sprintf("*.%s.%s.svc", svc, ns))
	}
	return append(out, n.clientHost(), fmt.Sprintf("%s.%s.svc", n.client(), ns), n.client(), "localhost")
}

func newKey() (*ecdsa.PrivateKey, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return key, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), nil
}

func serial() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 126))
}

type authority struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
	keyPEM  []byte
}

func newAuthority(name string) (*authority, error) {
	key, keyPEM, err := newKey()
	if err != nil {
		return nil, err
	}
	sn, err := serial()
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          sn,
		Subject:               pkix.Name{CommonName: name + " IsomerDB CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(caValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &authority{cert: cert, key: key, certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), keyPEM: keyPEM}, nil
}

func parseAuthority(certPEM, keyPEM []byte) (*authority, error) {
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, err
	}
	key, ok := pair.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("CA key is not ECDSA")
	}
	return &authority{cert: cert, key: key, certPEM: certPEM, keyPEM: keyPEM}, nil
}

// issue signs a leaf certificate: a server certificate for dnsNames, or a client certificate.
func (a *authority) issue(cn string, dnsNames []string, client bool) (certPEM, keyPEM []byte, err error) {
	key, keyPEM, err := newKey()
	if err != nil {
		return nil, nil, err
	}
	sn, err := serial()
	if err != nil {
		return nil, nil, err
	}
	usage := x509.ExtKeyUsageServerAuth
	if client {
		usage = x509.ExtKeyUsageClientAuth
	}
	tmpl := &x509.Certificate{
		SerialNumber: sn,
		Subject:      pkix.Name{CommonName: cn},
		DNSNames:     dnsNames,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(certValid),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, a.cert, &key.PublicKey, a.key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), keyPEM, nil
}

// needsReissue: the leaf is missing, expiring, signed by another CA, or for other names.
func needsReissue(data map[string][]byte, ca *authority, want []string) bool {
	block, _ := pem.Decode(data["tls.crt"])
	if block == nil || len(data["client.crt"]) == 0 || !bytes.Equal(data["ca.crt"], ca.certPEM) {
		return true
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil || time.Until(cert.NotAfter) < renewBefore || cert.CheckSignatureFrom(ca.cert) != nil {
		return true
	}
	if len(cert.DNSNames) != len(want) {
		return true
	}
	for i := range want {
		if cert.DNSNames[i] != want[i] {
			return true
		}
	}
	return false
}

// ensureTLS creates the CA once, and (re)issues the node certificates when needed. It returns
// the CA, and whether the node certificates changed (the nodes then reload their configuration).
func (r *IsomerClusterReconciler) ensureTLS(ctx context.Context, c *isomerv1.IsomerCluster) (*authority, *corev1.Secret, bool, error) {
	n := names{c}
	caSecret := &corev1.Secret{}
	var ca *authority
	err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: n.caSecret()}, caSecret)
	switch {
	case err == nil:
		if ca, err = parseAuthority(caSecret.Data["ca.crt"], caSecret.Data["ca.key"]); err != nil {
			return nil, nil, false, fmt.Errorf("secret %s: %w", n.caSecret(), err)
		}
	case apierrors.IsNotFound(err):
		if ca, err = newAuthority(c.Name); err != nil {
			return nil, nil, false, err
		}
		caSecret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: n.caSecret(), Namespace: c.Namespace, Labels: labels(c, roleCoordinator)},
			Data:       map[string][]byte{"ca.crt": ca.certPEM, "ca.key": ca.keyPEM},
		}
		if err := controllerutil.SetControllerReference(c, caSecret, r.Scheme); err != nil {
			return nil, nil, false, err
		}
		if err := r.Create(ctx, caSecret); err != nil {
			return nil, nil, false, err
		}
	default:
		return nil, nil, false, err
	}

	want := n.serverNames()
	leaf := &corev1.Secret{}
	err = r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: n.tlsSecret()}, leaf)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, nil, false, err
	}
	exists := err == nil
	if exists && !needsReissue(leaf.Data, ca, want) {
		return ca, leaf, false, nil
	}
	serverCrt, serverKey, err := ca.issue(c.Name+" nodes", want, false)
	if err != nil {
		return nil, nil, false, err
	}
	clientCrt, clientKey, err := ca.issue(nodeCertCN, nil, true)
	if err != nil {
		return nil, nil, false, err
	}
	data := map[string][]byte{
		"ca.crt": ca.certPEM, "tls.crt": serverCrt, "tls.key": serverKey, "client.crt": clientCrt, "client.key": clientKey,
	}
	if exists {
		leaf.Data = data
		return ca, leaf, true, r.Update(ctx, leaf)
	}
	leaf = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: n.tlsSecret(), Namespace: c.Namespace, Labels: labels(c, roleCoordinator)},
		Type:       corev1.SecretTypeOpaque,
		Data:       data,
	}
	if err := controllerutil.SetControllerReference(c, leaf, r.Scheme); err != nil {
		return nil, nil, false, err
	}
	return ca, leaf, true, r.Create(ctx, leaf)
}

// clientTLS is how the operator itself connects to nodes: verifying them, as the node identity.
func clientTLS(ca *authority, leaf *corev1.Secret, host string) (*tls.Config, error) {
	pair, err := tls.X509KeyPair(leaf.Data["client.crt"], leaf.Data["client.key"])
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	return &tls.Config{Certificates: []tls.Certificate{pair}, RootCAs: pool, ServerName: host, MinVersion: tls.VersionTLS12}, nil
}
