/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql.client;

import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;
import java.net.IDN;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Default hostname verifier.
 * <p>
 * Considers wildcards and IPv6 addresses.
 */
public enum DefaultHostnameVerifier implements HostnameVerifier {

    /**
     * Singleton instance.
     */
    INSTANCE;

    private static final int TYPE_DNS_NAME = 2;

    private static final int TYPE_IP_ADDRESS = 7;

    static Comparator<String> HOSTNAME_PATTERN_COMPARATOR = new Comparator<String>() {

        @Override
        public int compare(String o1, String o2) {
            // The more the dots the better: a.b.c.postgresql.org is more specific than postgresql.org
            int d1 = countChars(o1, '.');
            int d2 = countChars(o2, '.');
            if (d1 != d2) {
                return d1 > d2 ? 1 : -1;
            }

            // The less the stars the better: postgresql.org is more specific than *.*.postgresql.org
            int s1 = countChars(o1, '*');
            int s2 = countChars(o2, '*');
            if (s1 != s2) {
                return s1 < s2 ? 1 : -1;
            }

            // The longer the better: postgresql.org is more specific than sql.org
            int l1 = o1.length();
            int l2 = o2.length();
            if (l1 != l2) {
                return l1 > l2 ? 1 : -1;
            }

            return 0;
        }

        private int countChars(String value, char ch) {
            int count = 0;
            int pos = -1;
            while (true) {
                pos = value.indexOf(ch, pos + 1);
                if (pos == -1) {
                    break;
                }
                count++;
            }
            return count;
        }
    };

    private final Logger logger = Loggers.getLogger(this.getClass());

    @Override
    public boolean verify(String hostname, SSLSession session) {
        X509Certificate[] peerCerts;
        try {
            Certificate[] currentCerts = session.getPeerCertificates();
            peerCerts = new X509Certificate[currentCerts.length];
            for (int i=0;i<currentCerts.length;i++) {
                peerCerts[i] = (X509Certificate) currentCerts[i];
            }
        } catch (SSLPeerUnverifiedException e) {
            this.logger.warn("Unable to parse X509Certificate for hostname {}", hostname, e);
            return false;
        }
        if (peerCerts == null || peerCerts.length == 0) {
            this.logger.warn("No certificates found for hostname {}", hostname);
            return false;
        }

        String canonicalHostname;
        if (hostname.startsWith("[") && hostname.endsWith("]")) {
            // IPv6 address like [2001:db8:0:1:1:1:1:1]
            canonicalHostname = hostname.substring(1, hostname.length() - 1);
        } else {
            // This converts unicode domain name to ASCII
            try {
                canonicalHostname = IDN.toASCII(hostname);
                this.logger.debug("Canonical host name for {} is {}", hostname, canonicalHostname);
            } catch (IllegalArgumentException e) {
                // e.g. hostname is invalid
                this.logger.warn("Hostname {} is invalid", hostname, e);
                return false;
            }
        }

        X509Certificate serverCert = peerCerts[0];

        // Check for Subject Alternative Names (see RFC 6125)

        Collection<List<?>> subjectAltNames;
        try {
            subjectAltNames = serverCert.getSubjectAlternativeNames();
            if (subjectAltNames == null) {
                subjectAltNames = Collections.emptyList();
            }
        } catch (CertificateParsingException e) {
            this.logger.warn("Unable to parse certificates for hostname {}", hostname, e);
            return false;
        }

        boolean anyDnsSan = false;
        /*
         * Each item in the SAN collection is a 2-element list.
         * See {@link X509Certificate#getSubjectAlternativeNames}
         * The first element in each list is a number indicating the type of entry.
         */
        for (List<?> sanItem : subjectAltNames) {
            if (sanItem.size() != 2) {
                continue;
            }
            Integer sanType = (Integer) sanItem.get(0);
            if (sanType == null) {
                // just in case
                continue;
            }
            if (sanType != TYPE_IP_ADDRESS && sanType != TYPE_DNS_NAME) {
                continue;
            }
            String san = (String) sanItem.get(1);
            if (sanType == TYPE_IP_ADDRESS && san.startsWith("*")) {
                // Wildcards should not be present in the IP Address field
                continue;
            }
            anyDnsSan |= sanType == TYPE_DNS_NAME;
            if (verifyHostName(canonicalHostname, san)) {
                this.logger.debug("Server name validation pass for {}, subjectAltName {}", hostname, san);
                return true;
            }
        }

        if (anyDnsSan) {
            /*
             * RFC2818, section 3.1 (I bet you won't recheck :)
             * If a subjectAltName extension of type dNSName is present, that MUST
             * be used as the identity. Otherwise, the (most specific) Common Name
             * field in the Subject field of the certificate MUST be used. Although
             * the use of the Common Name is existing practice, it is deprecated and
             * Certification Authorities are encouraged to use the dNSName instead.
             */
            this.logger.debug("Server name validation failed: certificate for host {} dNSName entries subjectAltName, but none of them match. Assuming server name validation failed", hostname);
            return false;
        }

        // Last attempt: no DNS Subject Alternative Name entries detected, try common name
        LdapName DN;
        try {
            DN = new LdapName(serverCert.getSubjectX500Principal().getName(X500Principal.RFC2253));
        } catch (InvalidNameException e) {
            this.logger.warn("Server name validation failed: unable to extract common name from X509Certificate for hostname {}", hostname, e);
            return false;
        }

        List<String> commonNames = new ArrayList<String>(1);
        for (Rdn rdn : DN.getRdns()) {
            if ("CN".equals(rdn.getType())) {
                commonNames.add((String) rdn.getValue());
            }
        }
        if (commonNames.isEmpty()) {
            this.logger.warn("Server name validation failed: certificate for hostname {} has no DNS subjectAltNames, and it CommonName is missing as well", hostname);
            return false;
        }
        if (commonNames.size() > 1) {
            /*
             * RFC2818, section 3.1
             * If a subjectAltName extension of type dNSName is present, that MUST
             * be used as the identity. Otherwise, the (most specific) Common Name
             * field in the Subject field of the certificate MUST be used
             *
             * The sort is from less specific to most specific.
             */
            commonNames.sort(HOSTNAME_PATTERN_COMPARATOR);
        }
        String commonName = commonNames.get(commonNames.size() - 1);
        boolean result = verifyHostName(canonicalHostname, commonName);
        if (!result) {
            this.logger.warn("Server name validation failed: hostname {} does not match common name {}", hostname, commonName);
        }
        return result;
    }

    public boolean verifyHostName(@Nullable String hostname, @Nullable String pattern) {
        if (hostname == null || pattern == null) {
            return false;
        }
        int lastStar = pattern.lastIndexOf('*');
        if (lastStar == -1) {
            // No wildcard => just compare hostnames
            return hostname.equalsIgnoreCase(pattern);
        }
        if (lastStar > 0) {
            // Wildcards like foo*.com are not supported yet
            return false;
        }
        if (pattern.indexOf('.') == -1) {
            // Wildcard certificates should contain at least one dot
            return false;
        }
        // pattern starts with *, so hostname should be at least (pattern.length-1) long
        if (hostname.length() < pattern.length() - 1) {
            return false;
        }
        // Use case insensitive comparison
        final boolean ignoreCase = true;
        // Below code is "hostname.endsWithIgnoreCase(pattern.withoutFirstStar())"

        // E.g. hostname==sub.host.com; pattern==*.host.com
        // We need to start the offset of ".host.com" in hostname
        // For this we take hostname.length() - pattern.length()
        // and +1 is required since pattern is known to start with *
        int toffset = hostname.length() - pattern.length() + 1;

        // Wildcard covers just one domain level
        // a.b.c.com should not be covered by *.c.com
        if (hostname.lastIndexOf('.', toffset - 1) >= 0) {
            // If there's a dot in between 0..toffset
            return false;
        }

        return hostname.regionMatches(ignoreCase, toffset,
            pattern, 1, pattern.length() - 1);
    }

}
