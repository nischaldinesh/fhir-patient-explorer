"""SMART Backend Services authorization for the Bulk Data client (analytics/bulk.py): a signed JWT client assertion
(RS384, or ES384 for an EC key) traded for an access token with the client_credentials grant.

Configured only by environment variables, never by files in the repo:
  BULK_CLIENT_ID         the client id the server registered
  BULK_PRIVATE_KEY_PATH  path to the PEM private key (keep it outside the repo, for example under data/ or ~/.ssh)
  BULK_KEY_ID            the kid of the public key in the JWKS the server has (optional, many servers need it)
  BULK_TOKEN_URL         the token endpoint (default: token_endpoint of [base]/.well-known/smart-configuration)
  BULK_SCOPE             default system/*.read

Without BULK_CLIENT_ID nothing here runs and pyjwt is not needed (analytics/requirements-bulk-auth.txt).
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ASSERTION_TYPE = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"


def claims(client_id, token_url, now=None, lifetime=300, jti=None):
    """The assertion's claims (SMART Backend Services): issuer and subject are the client, the audience is the token
    endpoint, at most 5 minutes valid, a unique jti."""
    now = int(now if now is not None else time.time())
    return {"iss": client_id, "sub": client_id, "aud": token_url, "exp": now + min(lifetime, 300),
            "jti": jti or uuid.uuid4().hex}


def algorithm(private_key_pem):
    """RS384 for an RSA key, ES384 for an EC key (the two the spec requires servers to support)."""
    from cryptography.hazmat.primitives.asymmetric import ec, rsa
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    key = load_pem_private_key(private_key_pem, password=None)
    if isinstance(key, rsa.RSAPrivateKey):
        return "RS384"
    if isinstance(key, ec.EllipticCurvePrivateKey):
        return "ES384"
    raise ValueError("the private key must be RSA (RS384) or EC P-384 (ES384)")


def client_assertion(client_id, token_url, private_key_pem, kid=None, now=None, jti=None):
    import jwt
    headers = {"typ": "JWT"}
    if kid:
        headers["kid"] = kid
    return jwt.encode(claims(client_id, token_url, now, jti=jti), private_key_pem,
                      algorithm=algorithm(private_key_pem), headers=headers)


def token_form(scope, assertion):
    return urllib.parse.urlencode({"scope": scope, "grant_type": "client_credentials",
                                   "client_assertion_type": ASSERTION_TYPE, "client_assertion": assertion})


def post_form(url, body, timeout=60):
    req = urllib.request.Request(url, data=body.encode(), method="POST",
                                 headers={"Content-Type": "application/x-www-form-urlencoded",
                                          "Accept": "application/json"})
    try:
        from tools import urlopen
        with urlopen(req, timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        # the server's error text only: never the assertion
        raise RuntimeError(f"token request -> HTTP {e.code}: {e.read()[:300].decode(errors='replace')}") from None


def get_json(url, timeout=60):
    from tools import urlopen
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urlopen(req, timeout) as r:
        return json.loads(r.read())


def discover_token_url(base, fetch=get_json):
    conf = fetch(f"{base.rstrip('/')}/.well-known/smart-configuration")
    url = conf.get("token_endpoint")
    if not url:
        raise RuntimeError("the server's smart-configuration has no token_endpoint; set BULK_TOKEN_URL")
    return url


class BackendAuth:
    """Bearer token for bulk requests, fetched on first use and again 60 seconds before it expires."""

    def __init__(self, client_id, private_key_pem, token_url, scope="system/*.read", kid=None, post=post_form,
                 clock=time.time):
        self.client_id, self.key, self.token_url, self.scope, self.kid = client_id, private_key_pem, token_url, scope, kid
        self.post, self.clock = post, clock
        self.token, self.expires = None, 0.0

    def invalidate(self):
        self.token = None

    def headers(self):
        if not self.token or self.clock() > self.expires - 60:
            assertion = client_assertion(self.client_id, self.token_url, self.key, self.kid, now=self.clock())
            resp = self.post(self.token_url, token_form(self.scope, assertion))
            if not resp.get("access_token"):
                raise RuntimeError("token response without access_token")
            self.token = resp["access_token"]
            self.expires = self.clock() + float(resp.get("expires_in") or 300)
        return {"Authorization": f"Bearer {self.token}"}


def from_env(base, env=None, fetch=get_json):
    """A BackendAuth from BULK_* variables, or None when BULK_CLIENT_ID is not set."""
    env = os.environ if env is None else env
    client_id = env.get("BULK_CLIENT_ID")
    if not client_id:
        return None
    key_path = env.get("BULK_PRIVATE_KEY_PATH")
    if not key_path:
        raise RuntimeError("BULK_CLIENT_ID is set but BULK_PRIVATE_KEY_PATH is not")
    with open(key_path, "rb") as f:
        key = f.read()
    token_url = env.get("BULK_TOKEN_URL") or discover_token_url(base, fetch)
    return BackendAuth(client_id, key, token_url, env.get("BULK_SCOPE") or "system/*.read", env.get("BULK_KEY_ID"))
